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

    // Array of Skip Block to collect information about each skip block that create the current posting list
    protected ArrayList<SkipBlock> skipBlockArray = new ArrayList<>();
    protected int currentSkipBlock;

    protected Posting currentPosting;

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

        try {
            SkipBlockBlockManager sbm = new SkipBlockBlockManager(skipBlockPath, MODE.READ);
            // loop to collect skip blocks into an array
            for (int i = 0; i < vocabularyFileRecord.getHowManySkipBlocks(); i++) {
                SkipBlock sb = sbm.readRowAt(vocabularyFileRecord.getOffset() + i * SkipBlock.SKIP_BLOCK_ENTRY_SIZE);
                this.skipBlockArray.add(sb);
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

        if(nextRecordIndex % skipBlockMaxLen == 0 || nextRecordIndex == 0) {
            // load current skipblock
            SkipBlock sb = skipBlockArray.get(currentSkipBlock);
            try {
                this.nextRecordIndexInBlock = 0;

                docIdsDecompressed.clear();
                freqsDecompressed.clear();

                docIdsDecompressed.addAll( Arrays.stream(docIdsBinaryFileManager.readIntArray(sb.getDocIdByteSize(), sb.getDocIdFileOffset(), sb.getHowManyPostings())).boxed().toList() );
                freqsDecompressed.addAll( Arrays.stream(freqsBinaryFileManager.readIntArray(sb.getFreqByteSize(), sb.getFreqFileOffset(), sb.getHowManyPostings())).boxed().toList() ) ;
            } catch (Exception e) {
                e.printStackTrace();
            }
            currentSkipBlock += 1;
        }

        if( ! this.hasNext() ){
            //in this case I have already read all the Postings for this posting list,
            // then we return null
            return null;
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

        // move to the right skipBlock (skipping the ones that don't contain required docId)
        while(docId > this.skipBlockArray.get(currentSkipBlock).getMaxDocId()){
            // iterate until the current skipBlock contains the docId
            currentSkipBlock += 1;
        }

        SkipBlock sb = skipBlockArray.get(currentSkipBlock);
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

        int upperBound;
        if (currentSkipBlock < skipBlockArray.size()-1){
            upperBound = skipBlockMaxLen;
        } else{
            upperBound = howManyRecords - skipBlockMaxLen * currentSkipBlock;
        }

        int middle = lowerBound + ((upperBound - lowerBound) / 2);

        int middleDocId = docIdsDecompressed.get(middle);

        while(lowerBound != upperBound){

            if(middleDocId < docId){

                if(middle == lowerBound){
                    nextRecordIndexInBlock = upperBound;
                    int docuId = docIdsDecompressed.get(nextRecordIndexInBlock);
                    int freq = freqsDecompressed.get(nextRecordIndexInBlock);
                    return this.currentPosting = new Posting(docuId, freq);
                }

                lowerBound = middle;
                middle = lowerBound + ((upperBound - lowerBound) / 2);

            }else if(middleDocId == docId){
                nextRecordIndexInBlock = middle;
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
            int docuId = docIdsDecompressed.get(nextRecordIndexInBlock);
            int freq = freqsDecompressed.get(nextRecordIndexInBlock);
            return this.currentPosting = new Posting(docuId, freq);
        }else{
            //this.currentRecordIndex = this.howManyRecords - 1;
            return null;
        }
    }

    /**
     * resets the iterator to the first element of the ids and freqs of the posting list
     */
    protected void reset(){

        this.currentSkipBlock = 0;
        this.nextRecordIndex = 0;
        this.nextRecordIndexInBlock = 0;
        this.currentPosting = null;
    }

    @Override
    public void closeList() {
        this.freqsBinaryFileManager.close();
        this.freqsBinaryFileManager.close();
    }
}
