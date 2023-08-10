package queryProcessing.manager;

import common.bean.*;
import common.manager.block.SkipBlockBlockManager;
import common.manager.file.BinaryFileManager;
import common.manager.file.FileManager.*;
import common.manager.file.compression.DeltaCompressor;
import common.manager.file.compression.UnaryCompressor;
import config.ConfigLoader;

import java.io.IOException;
import java.io.ObjectInputFilter;
import java.util.ArrayList;
import java.util.Arrays;

public class PostingListIteratorTwoFile implements PostingListIterator {

    protected ArrayList<Integer> docIdsDecompressed = new ArrayList<>();
    protected ArrayList<Integer> freqsDecompressed = new ArrayList<>();

    protected int howManyRecords;
    protected int nextRecordIndex;
    protected int nextRecordIndexInBlock;


    protected int currentSkipBlockIndex;

    protected Posting currentPosting;

    protected int howManySkipBlocks;
    protected int firstSkipBlockOffset;
    protected SkipBlockBlockManager sbm;

    protected static String docIdsPath = ConfigLoader.getProperty("blocks.invertedindex.docIdFilePath") + ConfigLoader.getProperty("blocks.merged.invertedIndex.path");
    protected static String freqsPath = ConfigLoader.getProperty("blocks.invertedindex.freqFilePath") + ConfigLoader.getProperty("blocks.merged.invertedIndex.path");
    protected static String skipBlockPath = ConfigLoader.getProperty("blocks.merged.skipBlocks.path");
    protected BinaryFileManager docIdsBinaryFileManager;
    protected BinaryFileManager freqsBinaryFileManager;

    protected static int skipBlockMaxLen = ConfigLoader.getIntProperty("skipblocks.maxLen");

    @Override
    public void openList(VocabularyFileRecord vocabularyFileRecord) {

        this.docIdsBinaryFileManager = new BinaryFileManager(docIdsPath, MODE.READ, new DeltaCompressor());
        this.freqsBinaryFileManager = new BinaryFileManager(freqsPath, MODE.READ, new UnaryCompressor());

        this.howManyRecords = vocabularyFileRecord.getDf();

        this.howManySkipBlocks = vocabularyFileRecord.getHowManySkipBlocks();
        this.firstSkipBlockOffset = vocabularyFileRecord.getOffset();


        try {
            this.sbm = new SkipBlockBlockManager(skipBlockPath, MODE.READ);

        } catch (IOException e) {
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

        if(nextRecordIndex % skipBlockMaxLen == 0 || nextRecordIndex == 0) {
            // load current skipblock
            if(currentSkipBlockIndex >= this.howManySkipBlocks){
                // if the skip block is finished and I have to load another skip block but I have read all the skip blocks, the iterator is finished
                return null;
            }
            SkipBlock sb = this.getCurrentSkipBlock();
            try {
                this.nextRecordIndexInBlock = 0;

                docIdsDecompressed.clear();
                freqsDecompressed.clear();

                docIdsDecompressed.addAll( Arrays.stream(docIdsBinaryFileManager.readIntArray(sb.getDocIdByteSize(), sb.getDocIdFileOffset(), sb.getHowManyPostings())).boxed().toList() );
                freqsDecompressed.addAll( Arrays.stream(freqsBinaryFileManager.readIntArray(sb.getFreqByteSize(), sb.getFreqFileOffset(), sb.getHowManyPostings())).boxed().toList() ) ;
            } catch (Exception e) {
                e.printStackTrace();
            }
            currentSkipBlockIndex += 1;
        }



        int docId = docIdsDecompressed.get(nextRecordIndexInBlock);
        int freq = freqsDecompressed.get(nextRecordIndexInBlock);

        this.nextRecordIndexInBlock += 1;
        this.nextRecordIndex += 1;

        return this.currentPosting = new Posting(docId, freq);
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

        //TODO: if the current skip block's max docID is >= docId
        // for the current implementation it is loaded the first posting whose docID is >= of docID,
        // even when that posting comes before the previous current posting
        // this could cause problems because can rewind the iterator

        // move to the right skipBlock (skipping the ones that don't contain required docId)
        SkipBlock sb = null;
        while(docId > (sb = this.getCurrentSkipBlock()).getMaxDocId()){
            // iterate until the current skipBlock contains the docId
            if(this.hasNextSkipBlock()){
                currentSkipBlockIndex += 1;
            }else{
                //the skip blocks are finished and I have not found a Posting whose docId is >= docId
                this.nextRecordIndex = this.howManyRecords;
                return null;
            }
        }

        try {
            this.nextRecordIndexInBlock = 0;

            docIdsDecompressed.clear();
            freqsDecompressed.clear();

            docIdsDecompressed.addAll( Arrays.stream(docIdsBinaryFileManager.readIntArray(sb.getDocIdByteSize(), sb.getDocIdFileOffset(), sb.getHowManyPostings())).boxed().toList() );
            freqsDecompressed.addAll( Arrays.stream(freqsBinaryFileManager.readIntArray(sb.getFreqByteSize(), sb.getFreqFileOffset(), sb.getHowManyPostings())).boxed().toList() ) ;
        } catch (Exception e) {
            e.printStackTrace();
        }

        //we can perform binary search to retrieve the first Posting GEQ

        int lowerBound = 0;

        int upperBound = sb.getHowManyPostings() - 1;


        int middle = lowerBound + ((upperBound - lowerBound) / 2);

        int middleDocId = docIdsDecompressed.get(middle);

        

        while(lowerBound != upperBound){

            if(middleDocId < docId){

                if(middle == lowerBound){
                    nextRecordIndexInBlock = upperBound;
                    this.nextRecordIndex = this.currentSkipBlockIndex * PostingListIteratorTwoFile.skipBlockMaxLen + this.nextRecordIndexInBlock;
                    int docuId = docIdsDecompressed.get(nextRecordIndexInBlock);
                    int freq = freqsDecompressed.get(nextRecordIndexInBlock);
                    return this.currentPosting = new Posting(docuId, freq); //TO CHECK
                }

                lowerBound = middle;
                middle = lowerBound + ((upperBound - lowerBound) / 2);

            }else if(middleDocId == docId){
                nextRecordIndexInBlock = middle;
                this.nextRecordIndex = this.currentSkipBlockIndex * PostingListIteratorTwoFile.skipBlockMaxLen + this.nextRecordIndexInBlock;
                int docuId = docIdsDecompressed.get(nextRecordIndexInBlock);
                int freq = freqsDecompressed.get(nextRecordIndexInBlock);
                return this.currentPosting = new Posting(docuId, freq);
            }else{
                // when we have middlePosting.getDocid() > docId
                upperBound = middle;
                middle = lowerBound + ((upperBound - lowerBound) / 2);
            }
            middleDocId = docIdsDecompressed.get(middle);
        }


        if(middleDocId >= docId){
            nextRecordIndexInBlock = middle;
            this.nextRecordIndex = this.currentSkipBlockIndex * PostingListIteratorTwoFile.skipBlockMaxLen + this.nextRecordIndexInBlock;
            int docuId = docIdsDecompressed.get(nextRecordIndexInBlock);
            int freq = freqsDecompressed.get(nextRecordIndexInBlock);
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
    protected void reset(){

        this.currentSkipBlockIndex = 0;
        this.nextRecordIndex = 0;
        this.nextRecordIndexInBlock = 0;
        this.currentPosting = null;
    }

    @Override
    public void closeList() {
        this.freqsBinaryFileManager.close();
        this.freqsBinaryFileManager.close();
    }

    private SkipBlock getSkipBlockAt(int index){
        if(index >= this.howManySkipBlocks){
            return null;
        }
        try {
            return this.sbm.readRowAt(this.firstSkipBlockOffset + index * SkipBlock.SKIP_BLOCK_ENTRY_SIZE);
        } catch (Exception e) {
            System.out.println("getSkipBlock cannot read skipBlock row");
            e.printStackTrace();
            return null;
        }
    }
    private boolean hasNextSkipBlock(){
        return this.currentSkipBlockIndex < this.howManySkipBlocks;
    }
    private SkipBlock getNextSkipBlock(){
        this.currentSkipBlockIndex += 1;
        return this.getSkipBlockAt(this.currentSkipBlockIndex);
    }
    private SkipBlock getCurrentSkipBlock(){
        return this.getSkipBlockAt(this.currentSkipBlockIndex);
    }
}
