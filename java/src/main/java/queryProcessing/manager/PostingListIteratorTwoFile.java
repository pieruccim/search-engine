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

    //protected static final int skipBlockMaxLen = ConfigLoader.getIntProperty("skipblocks.maxLen");
    public static final boolean useCache = ConfigLoader.getPropertyBool("performance.iterators.useCache");
    public static final boolean useThreads = ConfigLoader.getPropertyBool("performance.iterators.useThreads");
    public static final int howManyThreads = ConfigLoader.getIntProperty("performance.iterators.threads.howMany");
    public static final int cacheSize = ConfigLoader.getIntProperty("performance.iterators.cache.size");
    public static final boolean debug = ConfigLoader.getPropertyBool("performance.iterators.debug");

    protected int[] docIdsDecompressed;
    protected int[] freqsDecompressed ;

    private static final ExecutorService executor = useThreads ? Executors.newFixedThreadPool(howManyThreads) : null;

    protected int howManyRecords;
    protected int nextRecordIndex;
    protected int nextRecordIndexInBlock;


    protected int currentSkipBlockIndex;

    protected Posting currentPosting;

    protected SkipBlock[] skipBlockArray;
    protected int howManySkipBlocks;
    protected int firstSkipBlockOffset;
    protected String term;
    protected SkipBlockBlockManager sbm;

    protected static final boolean loadSkipBlocksInMemory = true;

    protected static String docIdsPath = ConfigLoader.getProperty("blocks.invertedindex.docIdFilePath") + ConfigLoader.getProperty("blocks.merged.invertedIndex.path");
    protected static String freqsPath = ConfigLoader.getProperty("blocks.invertedindex.freqFilePath") + ConfigLoader.getProperty("blocks.merged.invertedIndex.path");
    protected static String skipBlockPath = ConfigLoader.getProperty("blocks.merged.skipBlocks.path");
    protected BinaryFileManager docIdsBinaryFileManager;
    protected BinaryFileManager freqsBinaryFileManager;

    protected BinaryFileManager thDocIdsBinaryFileManager;
    protected BinaryFileManager thFreqsBinaryFileManager;

    static final private boolean useCompression = ConfigLoader.getPropertyBool("invertedIndex.useCompression");

    private BinaryFileManager createNewDocIdsFileManager(){
        if(useCompression){
            return new BinaryFileManager(docIdsPath, MODE.READ, new DeltaCompressor());
        }else{
            return new BinaryFileManager(docIdsPath, MODE.READ);
        }
    }

    private BinaryFileManager createNewFreqsFileManager(){
        if(useCompression){
            return new BinaryFileManager(freqsPath, MODE.READ, new UnaryCompressor());
        }else{
            return new BinaryFileManager(freqsPath, MODE.READ);
        }
    }

    @Override
    public void openList(VocabularyFileRecord vocabularyFileRecord) {
        //System.out.println("useCache: " + useCache + "useThreads: " + useThreads);

        this.docIdsBinaryFileManager = this.createNewDocIdsFileManager();
        this.freqsBinaryFileManager = this.createNewFreqsFileManager();

        if(useThreads){
            this.thDocIdsBinaryFileManager = this.createNewDocIdsFileManager();
            this.thFreqsBinaryFileManager = this.createNewFreqsFileManager();
        }

        this.howManyRecords = vocabularyFileRecord.getDf();

        this.howManySkipBlocks = vocabularyFileRecord.getHowManySkipBlocks();
        this.firstSkipBlockOffset = vocabularyFileRecord.getOffset();
        this.term = vocabularyFileRecord.getTerm();


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

        if(this.reachedBlockEnd()) {
            // load current skipblock

            if(nextRecordIndex == 0){
                // case in which I am reading the first posting of the first block
                currentSkipBlockIndex = 0;
            }else{
                if( ! hasNextSkipBlock()){
                    // if the skip block is finished and I have to load another skip block 
                    // but I have read all the skip blocks, the iterator is finished
                    return null;
                }
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

    private boolean reachedBlockEnd(){
        if(this.loadedSkipBlock == null || this.docIdsDecompressed == null){
            // case in which there is no block currently loaded
            return true;
        }
        if(this.nextRecordIndexInBlock >= this.docIdsDecompressed.length){
            return true;
        }
        return false;
    }

    protected LRUCache<SkipBlock, Pair<int[], int[]>> cache = useCache ? new LRUCache<SkipBlock, Pair<int[], int[]>>(cacheSize, null) : null;

    public class LoadingPostingListBlock implements Callable<Pair<int[], int[]>> {
        protected SkipBlock nextSB;
        BinaryFileManager threadDocIdsBinaryFileManager;
        BinaryFileManager threadFreqsBinaryFileManager;

        public LoadingPostingListBlock(SkipBlock nextSB) {
            this.nextSB = nextSB;
            if(threadDocIdsBinaryFileManager == null){
                //System.out.println("creating binaryFileManagers for threads for term: '" + term + "'");
                this.threadDocIdsBinaryFileManager = thDocIdsBinaryFileManager;
                this.threadFreqsBinaryFileManager  = thFreqsBinaryFileManager;
            }
        }

        @Override
        public Pair<int[], int[]> call(){
            if(this.nextSB == null){
                if(debug) System.out.println("Called a thread passing a null parameter");
                return null;
            }

            int [] docIdsDecompressedNextBlock;
            int [] freqsDecompressedNextBlock;

            try {
                docIdsDecompressedNextBlock = threadDocIdsBinaryFileManager.readIntArray(this.nextSB.getDocIdByteSize(), this.nextSB.getDocIdFileOffset(), this.nextSB.getHowManyPostings());
                freqsDecompressedNextBlock  = threadFreqsBinaryFileManager.readIntArray(this.nextSB.getFreqByteSize(), this.nextSB.getFreqFileOffset(), this.nextSB.getHowManyPostings()   );
            } catch (Exception e) {
                if(debug){
                e.printStackTrace();
                System.out.println("Exception in thread for term iterator: " + term);
                System.out.println("SkipBlock that was trying to read: " + this.nextSB.toString());
                }
                return null;
            }
            Pair<int[], int[]> ret = new Pair<int[], int[]>(docIdsDecompressedNextBlock, freqsDecompressedNextBlock);
    
            //if(useCache){
            //    cache.put(this.nextSB, ret);
            //}
            return ret;
        }
    }

    //private Future<Pair<int[], int[]>> future;
    //private LoadingPostingListBlock loader;

    private Pair<LoadingPostingListBlock, Future<Pair<int[], int[]>>> threadInfo;

    private SkipBlock loadedSkipBlock;

    private final int LOADING_NUM_RETRIES = 2;
    /**
     * resets nextRecordIndexInBlock to 0
     * loads docIds and frequencies buffer for the current skip block
     * @return false in case of error
     */
    private boolean loadPostingListCurrentSkipBlock(){
        return this.loadPostingListCurrentSkipBlock(LOADING_NUM_RETRIES);
    }
    private boolean loadPostingListCurrentSkipBlock(int numRetries){
        SkipBlock sb = this.getCurrentSkipBlock();

        if(sb == null){
            System.out.println("[loadPostingListCurrentSkipBlock] : trying to load non-existing block");
            return false;
        }

        this.nextRecordIndexInBlock = 0;

        if(sb.equals(loadedSkipBlock)
                && docIdsDecompressed != null
                && docIdsDecompressed.length == sb.getHowManyPostings()
                && docIdsDecompressed[docIdsDecompressed.length - 1] == sb.getMaxDocId()){
            // case in which the currentSkipBlock was already loaded
            return true;
        }

        Pair<int[], int[]> outcome = (useCache) ? this.cache.get(sb) : null;

        if(outcome != null){
            this.docIdsDecompressed = outcome.getKey();
            this.freqsDecompressed  = outcome.getValue();
        }
        else if(outcome == null && useThreads){
            try {

                if (this.threadInfo != null && this.threadInfo.getKey().nextSB.equals(sb) && (outcome = this.threadInfo.getValue().get()) != null){

                    this.docIdsDecompressed = outcome.getKey();
                    this.freqsDecompressed  = outcome.getValue();
                    
                    this.threadInfo = null;

                    if(useCache){
                        this.cache.put(sb, outcome);
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                System.out.println("Current term: " + term);
            }
        }
        if(outcome == null){

            if(useThreads && this.threadInfo != null){
                this.threadInfo.getValue().cancel(true);
                this.threadInfo = null;
            }

            try {
                int[] tmpDocIds     = docIdsBinaryFileManager.readIntArray(sb.getDocIdByteSize(), sb.getDocIdFileOffset(), sb.getHowManyPostings());    
                int[] tmpDocFreq    = freqsBinaryFileManager.readIntArray(sb.getFreqByteSize(), sb.getFreqFileOffset(), sb.getHowManyPostings());
                docIdsDecompressed  = tmpDocIds;
                freqsDecompressed   = tmpDocFreq;
                if(useCache){
                    this.cache.put(sb, new Pair<int[], int[]>(tmpDocIds, tmpDocFreq));
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Exception in Iterator main thread, current term: " + term + " num retries left: " + numRetries);
                this.loadedSkipBlock = null;
                this.docIdsDecompressed = null;
                this.freqsDecompressed = null;
                if(numRetries > 0){
                    return this.loadPostingListCurrentSkipBlock(numRetries - 1);
                }
                System.exit(-1);
                return false;
            }

        }

        if(this.hasNextSkipBlock() && useThreads){
                if(this.threadInfo != null){
                    this.threadInfo.getValue().cancel(true);
                    this.threadInfo = null;
                }

                SkipBlock nextBlock = this.getSkipBlockAt(currentSkipBlockIndex + 1);
                LoadingPostingListBlock loader = new LoadingPostingListBlock(nextBlock);
                Future<Pair<int[],int[]>> future = executor.submit(loader);
                this.threadInfo = new Pair<LoadingPostingListBlock, Future<Pair<int[],int[]>>>(loader, future);

        }
        //if(sb.getHowManyPostings() == this.docIdsDecompressed.length 
        //    && sb.getMaxDocId() == this.docIdsDecompressed[sb.getHowManyPostings() - 1]){
        this.loadedSkipBlock = sb;
        return true;
        //}else{
        //    if(numRetries == LOADING_NUM_RETRIES){
        //        System.out.println();
        //    }
        //    System.out.println("[+++] loaded wrong data [+++] for term '" + term + "' num retries left: " + numRetries + 
        //    " currentSBIndex: " + currentSkipBlockIndex + " howManySB: " + howManySkipBlocks + 
        //    " sb.HowManyPostings: " + sb.getHowManyPostings() + " sb.maxdocid: " + sb.getMaxDocId() +
        //    " loaded data size: "+ this.docIdsDecompressed.length + " maxdocid loaded: " + this.docIdsDecompressed[this.docIdsDecompressed.length - 1]);
        //    if(numRetries > 0)
        //        return this.loadPostingListCurrentSkipBlock(numRetries - 1);
        //    System.out.println("[x] max num retries exceeded, exiting ... [x]");
        //    System.exit(0);
        //    return false;
        //}

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
        return this.nextGEQ(docId, 1);
    }

    private Posting nextGEQ(long docId, int numRetries) {

        Posting currPosting = this.getCurrentPosting();

        if(currPosting != null && currPosting.getDocid() >= docId){
            return this.next();
        }

        SkipBlock sb = this.getCurrentSkipBlock();

        if(sb.getMaxDocId() < docId || this.getCurrentPosting() == null){   // this.getCurrentPosting() == null case in which the postings block has still to be loaded
            // move to the right skipBlock (skipping the ones that don't contain required docId)
            
            this.nextRecordIndex -= this.nextRecordIndexInBlock;
            
            while(docId > sb.getMaxDocId()){
                // iterate until the current skipBlock contains the docId
                if(this.hasNextSkipBlock()){
                    
                    this.nextRecordIndex += sb.getHowManyPostings();
                    currentSkipBlockIndex += 1;
                    sb = this.getCurrentSkipBlock();
                    
                }else{
                    //the skip blocks are finished and I have not found a Posting whose docId is >= requested docId
                    this.nextRecordIndex = this.howManyRecords;
                    return null;
                }
            }

            this.loadPostingListCurrentSkipBlock();
        }

        //we can perform binary search to retrieve the first Posting GEQ
        this.nextRecordIndex -= this.nextRecordIndexInBlock;

        int lowerBound = 0;

        int upperBound = sb.getHowManyPostings() - 1;


        int middle = lowerBound + ((upperBound - lowerBound) / 2);

        int middleDocId = docIdsDecompressed[middle];

        while(lowerBound != upperBound){

            if(middleDocId < docId){

                if(middle == lowerBound){
                    nextRecordIndexInBlock = upperBound;
                    this.nextRecordIndex += this.nextRecordIndexInBlock;
                    int docuId = docIdsDecompressed[nextRecordIndexInBlock];
                    int freq = freqsDecompressed[nextRecordIndexInBlock];
                    if(docuId < docId){
                        System.out.println("error!" + "nextGEQ(" + docId + ") docuId returned: " + docuId + " indexInBlock: " + nextRecordIndexInBlock +
                            " upperBound: " + upperBound + " sb.maxdocid: " + sb.getMaxDocId() + "term: " + term + " numretries: " + numRetries);
                        //System.exit(0);
                        if(numRetries > 0){
                            Posting ret = this.nextGEQ(middleDocId, numRetries - 1);
                            System.out.println("Posting returned after numretries " + numRetries + " left: " + ret.toString());
                            return ret;
                        }
                        //System.exit(-1);
                        return null;
                    }
                    this.nextRecordIndex++;
                    this.nextRecordIndexInBlock++;
                    return this.currentPosting = new Posting(docuId, freq);
                }

                lowerBound = middle;
                middle = lowerBound + ((upperBound - lowerBound) / 2);

            }else if(middleDocId == docId){
                nextRecordIndexInBlock = middle;
                this.nextRecordIndex += this.nextRecordIndexInBlock;
                int docuId = docIdsDecompressed[nextRecordIndexInBlock];
                int freq = freqsDecompressed[nextRecordIndexInBlock];
                this.nextRecordIndex++;
                this.nextRecordIndexInBlock++;
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
            this.nextRecordIndex += this.nextRecordIndexInBlock;
            int docuId = docIdsDecompressed[nextRecordIndexInBlock];
            int freq = freqsDecompressed[nextRecordIndexInBlock];
            nextRecordIndex++;
            nextRecordIndexInBlock++;
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
        this.loadedSkipBlock = null;
        this.docIdsDecompressed = null;
        this.freqsDecompressed = null;
        if(useThreads && this.threadInfo != null){
            this.threadInfo.getValue().cancel(true);
        }
        this.threadInfo = null;

        this.loadPostingListCurrentSkipBlock();
    }

    @Override
    public void close() {
        this.freqsBinaryFileManager.close();
        this.freqsBinaryFileManager.close();
        if(this.threadInfo != null){
            this.threadInfo.getValue().cancel(true);
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
    private SkipBlock getCurrentSkipBlock(){
        return this.getSkipBlockAt(this.currentSkipBlockIndex);
    }
}
