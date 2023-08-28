package queryProcessing.manager;

import common.bean.*;
import common.manager.block.SkipBlockBlockManager;
import common.manager.file.BinaryFileManager;
import common.manager.file.FileManager.*;
import common.manager.file.compression.DeltaCompressor;
import common.manager.file.compression.UnaryCompressor;
import common.utils.LRUCache;
import config.ConfigLoader;
import javafx.util.Pair;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PostingListIteratorTwoFile implements PostingListIterator {

    protected static final int skipBlockMaxLen = ConfigLoader.getIntProperty("skipblocks.maxLen");
    protected static final boolean useCache = true;
    protected static final boolean useThreads = true;

    protected int[] docIdsDecompressed;
    protected int[] freqsDecompressed ;

    private static final ExecutorService executor = useThreads ? Executors.newFixedThreadPool(20) : null;

    protected int howManyRecords;
    protected int nextRecordIndex;
    protected int nextRecordIndexInBlock;


    protected int currentSkipBlockIndex;

    protected Posting currentPosting;

    protected SkipBlock[] skipBlockArray;
    protected int howManySkipBlocks;
    protected int firstSkipBlockOffset;
    protected SkipBlockBlockManager sbm;

    protected static final boolean loadSkipBlocksInMemory = true;

    protected static String docIdsPath = ConfigLoader.getProperty("blocks.invertedindex.docIdFilePath") + ConfigLoader.getProperty("blocks.merged.invertedIndex.path");
    protected static String freqsPath = ConfigLoader.getProperty("blocks.invertedindex.freqFilePath") + ConfigLoader.getProperty("blocks.merged.invertedIndex.path");
    protected static String skipBlockPath = ConfigLoader.getProperty("blocks.merged.skipBlocks.path");
    protected BinaryFileManager docIdsBinaryFileManager;
    protected BinaryFileManager freqsBinaryFileManager;

    static final private boolean useCompression = ConfigLoader.getPropertyBool("invertedIndex.useCompression");

    @Override
    public void openList(VocabularyFileRecord vocabularyFileRecord) {
        if(useCompression){
            this.docIdsBinaryFileManager = new BinaryFileManager(docIdsPath, MODE.READ, new DeltaCompressor());
            this.freqsBinaryFileManager = new BinaryFileManager(freqsPath, MODE.READ, new UnaryCompressor());
        }else{
            this.docIdsBinaryFileManager = new BinaryFileManager(docIdsPath, MODE.READ);
            this.freqsBinaryFileManager = new BinaryFileManager(freqsPath, MODE.READ);
        }
        this.howManyRecords = vocabularyFileRecord.getDf();

        this.howManySkipBlocks = vocabularyFileRecord.getHowManySkipBlocks();
        this.firstSkipBlockOffset = vocabularyFileRecord.getOffset();


        try {
            this.sbm = new SkipBlockBlockManager(skipBlockPath, MODE.READ);

            if(loadSkipBlocksInMemory){

                this.skipBlockArray = new SkipBlock[this.howManySkipBlocks];

                for (int i = 0; i < this.howManySkipBlocks; i++) {
                    SkipBlock sb = this.sbm.readRowAt(this.firstSkipBlockOffset + i * SkipBlock.SKIP_BLOCK_ENTRY_SIZE);
                    this.skipBlockArray[i] = sb;
                }

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        reset();
    }

    @Override
    public Posting next() {

        if( ! this.hasNext() ){
            //in this case I have already read all the Postings for this posting list,
            // then we return null
            return null;
        }

        if(nextRecordIndex % skipBlockMaxLen == 0 ) {    // || nextRecordIndexInBlock == docIdsDecompressed.length
            // load current skipblock
            if(currentSkipBlockIndex >= this.howManySkipBlocks){
                // if the skip block is finished and I have to load another skip block but I have read all the skip blocks, the iterator is finished
                return null;
            }
            if(nextRecordIndex == 0){
                currentSkipBlockIndex = 0;
            }else{
                currentSkipBlockIndex += 1;
            }

            this.loadPostingListCurrentSkipBlock();
        }



        int docId = docIdsDecompressed[nextRecordIndexInBlock];
        int freq = freqsDecompressed[nextRecordIndexInBlock];

        this.nextRecordIndexInBlock += 1;
        this.nextRecordIndex += 1;

        return this.currentPosting = new Posting(docId, freq);
    }

    protected LRUCache<SkipBlock, Pair<int[], int[]>> cache = useCache ? new LRUCache<SkipBlock, Pair<int[], int[]>>(128, null) : null;

    public class LoadingPostingListBlock implements Callable<Pair<int[], int[]>> {
        protected SkipBlock nextSB;
        BinaryFileManager docIdsBinaryFileManager;
        BinaryFileManager freqsBinaryFileManager;

        public LoadingPostingListBlock(SkipBlock nextSB, BinaryFileManager docIdsBinaryFileManager, BinaryFileManager freqsBinaryFileManager) {
            this.nextSB = nextSB;
            this.docIdsBinaryFileManager = docIdsBinaryFileManager;
            this.freqsBinaryFileManager  = freqsBinaryFileManager;
        }

        @Override
        public Pair<int[], int[]> call(){
            if(nextSB == null){
                System.out.println("Called a thread passing a null parameter");
                return null;
            }

            int [] docIdsDecompressedNextBlock;
            int [] freqsDecompressedNextBlock;

            try {
                docIdsDecompressedNextBlock = docIdsBinaryFileManager.readIntArray(nextSB.getDocIdByteSize(), nextSB.getDocIdFileOffset(), nextSB.getHowManyPostings());
                freqsDecompressedNextBlock  = freqsBinaryFileManager.readIntArray(nextSB.getFreqByteSize(), nextSB.getFreqFileOffset(), nextSB.getHowManyPostings()   );
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
            Pair<int[], int[]> ret = new Pair<int[], int[]>(docIdsDecompressedNextBlock, freqsDecompressedNextBlock);
    
            if(useCache)
                cache.put(nextSB, ret);

            return ret;
        }
    }

    private Future<Pair<int[], int[]>> future;
    private LoadingPostingListBlock loader;
    /**
     * resets nextRecordIndexInBlock to 0
     * loads docIds and frequencies buffer for the current skip block
     * @return false in case of error
     */
    private boolean loadPostingListCurrentSkipBlock(){
        SkipBlock sb = this.getCurrentSkipBlock();

        if(sb == null){
            System.out.println("[loadPostingListCurrentSkipBlock] : trying to load non-existing block");
            return false;
        }

        this.nextRecordIndexInBlock = 0;

        Pair<int[], int[]> outcome = (useCache) ? this.cache.get(sb) : null;

        if(outcome == null && useThreads){
            try {

                if (future != null && loader.nextSB == sb && future.get() != null){
                    outcome = future.get();
                    if(outcome != null){
                        this.docIdsDecompressed = outcome.getKey();
                        this.freqsDecompressed  = outcome.getValue();
                    }
                    future = null;
                    loader = null;
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }


        if(outcome == null){

            try {
                docIdsDecompressed = docIdsBinaryFileManager.readIntArray(sb.getDocIdByteSize(), sb.getDocIdFileOffset(), sb.getHowManyPostings());
                freqsDecompressed  = freqsBinaryFileManager.readIntArray(sb.getFreqByteSize(), sb.getFreqFileOffset(), sb.getHowManyPostings()   );

                if(useCache){
                    this.cache.put(sb, new Pair<int[], int[]>(docIdsDecompressed, freqsDecompressed));
                }
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }

        }

        if(this.hasNextSkipBlock() && useThreads){

                SkipBlock nextSB = this.getSkipBlockAt(currentSkipBlockIndex + 1);
                this.loader = new LoadingPostingListBlock(nextSB, docIdsBinaryFileManager, freqsBinaryFileManager);
                this.future = executor.submit(loader);

        }

        return true;
    }

    @Override
    public boolean hasNext() {
        return this.nextRecordIndex < this.howManyRecords;
    }

    @Override
    public Posting getCurrentPosting() {
        return this.currentPosting;
    }

    @Override
    public Posting nextGEQ(long docId) {

        Posting currPosting = this.getCurrentPosting();

        if(currPosting != null && currPosting.getDocid() >= docId){
            return this.next();
        }

        SkipBlock sb = this.getCurrentSkipBlock();

        if(sb.getMaxDocId() < docId || this.getCurrentPosting() == null){   // this.getCurrentPosting() == null case in which the postings block has still to be loaded
            // move to the right skipBlock (skipping the ones that don't contain required docId)
            
            while(docId > sb.getMaxDocId()){
                // iterate until the current skipBlock contains the docId
                if(this.hasNextSkipBlock()){
                    currentSkipBlockIndex += 1;
                    sb = this.getCurrentSkipBlock();
                }else{
                    //the skip blocks are finished and I have not found a Posting whose docId is >= docId
                    this.nextRecordIndex = this.howManyRecords;
                    return null;
                }
            }

            this.nextRecordIndex = this.currentSkipBlockIndex * PostingListIteratorTwoFile.skipBlockMaxLen;

            this.loadPostingListCurrentSkipBlock();
        }

        //we can perform binary search to retrieve the first Posting GEQ

        int lowerBound = 0;

        int upperBound = sb.getHowManyPostings() - 1;


        int middle = lowerBound + ((upperBound - lowerBound) / 2);

        int middleDocId = docIdsDecompressed[middle];

        while(lowerBound != upperBound){

            if(middleDocId < docId){

                if(middle == lowerBound){
                    nextRecordIndexInBlock = upperBound;
                    this.nextRecordIndex = this.currentSkipBlockIndex * PostingListIteratorTwoFile.skipBlockMaxLen + this.nextRecordIndexInBlock;
                    int docuId = docIdsDecompressed[nextRecordIndexInBlock];
                    int freq = freqsDecompressed[nextRecordIndexInBlock];
                    if(docuId < docId){
                        System.out.println("error!" + "nextGEQ(" + docId + ") docuId returned: " + docuId + " indexInBlock: " + nextRecordIndexInBlock +
                            " upperBound: " + upperBound + " sb.maxdocid: " + sb.getMaxDocId());
                        //System.exit(0);
                    }
                    return this.currentPosting = new Posting(docuId, freq);
                }

                lowerBound = middle;
                middle = lowerBound + ((upperBound - lowerBound) / 2);

            }else if(middleDocId == docId){
                nextRecordIndexInBlock = middle;
                this.nextRecordIndex = this.currentSkipBlockIndex * PostingListIteratorTwoFile.skipBlockMaxLen + this.nextRecordIndexInBlock;
                int docuId = docIdsDecompressed[nextRecordIndexInBlock];
                int freq = freqsDecompressed[nextRecordIndexInBlock];
                return this.currentPosting = new Posting(docuId, freq);
            }else{
                // when we have middlePosting.getDocid() > docId
                upperBound = middle;
                middle = lowerBound + ((upperBound - lowerBound) / 2);
            }
            middleDocId = docIdsDecompressed[middle];
        }


        if(middleDocId >= docId){
            nextRecordIndexInBlock = middle;
            this.nextRecordIndex = this.currentSkipBlockIndex * PostingListIteratorTwoFile.skipBlockMaxLen + this.nextRecordIndexInBlock;
            int docuId = docIdsDecompressed[nextRecordIndexInBlock];
            int freq = freqsDecompressed[nextRecordIndexInBlock];
            return this.currentPosting = new Posting(docuId, freq);
        }else{
            this.nextRecordIndexInBlock = sb.getHowManyPostings();
            this.nextRecordIndex = this.howManyRecords; // in order to store the fact that the the iterator is finished
            return null;
        }
    }

    /**
     * resets the iterator to the first element of the ids and freqs of the posting list
     */
    @Override
    public void reset(){

        this.currentSkipBlockIndex = 0;
        this.nextRecordIndex = 0;
        this.nextRecordIndexInBlock = 0;
        this.currentPosting = null;
        this.future = null;
    }

    @Override
    public void closeList() {
        this.freqsBinaryFileManager.close();
        this.freqsBinaryFileManager.close();
        if(this.future != null){
            this.future.cancel(true);
        }
    }

    /**
     * you must call this method once before program termination
     * once this method is called, you cannot perform any more queries
     */
    public static void shutdownThreads(){
        if(useThreads)
            executor.shutdown();
    }

    private SkipBlock getSkipBlockAt(int index){
        if(index >= this.howManySkipBlocks){
            //System.out.println("[getSkipBlockAt]: returning null, given index: " + index + " howManySkipBlocks: " + this.howManySkipBlocks);
            return null;
        }
        if(loadSkipBlocksInMemory){
            return this.skipBlockArray[index];
        }
        else{
            try {
                return this.sbm.readRowAt(this.firstSkipBlockOffset + index * SkipBlock.SKIP_BLOCK_ENTRY_SIZE);
            } catch (Exception e) {
                System.out.println("getSkipBlock cannot read skipBlock row");
                e.printStackTrace();
                return null;
            }
        }
    }
    private boolean hasNextSkipBlock(){
        return this.currentSkipBlockIndex + 1 < this.howManySkipBlocks;
    }
    private SkipBlock getNextSkipBlock(){
        this.currentSkipBlockIndex += 1;
        return this.getSkipBlockAt(this.currentSkipBlockIndex);
    }
    private SkipBlock getCurrentSkipBlock(){
        return this.getSkipBlockAt(this.currentSkipBlockIndex);
    }
}
