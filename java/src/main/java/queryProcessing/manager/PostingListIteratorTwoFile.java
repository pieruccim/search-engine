package queryProcessing.manager;

import common.bean.*;
import common.manager.block.SkipBlockBlockManager;
import common.manager.file.BinaryFileManager;
import common.manager.file.FileManager.*;
import common.manager.file.compression.DeltaCompressor;
import common.manager.file.compression.UnaryCompressor;
import config.ConfigLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class PostingListIteratorTwoFile implements PostingListIterator {

    protected ArrayList<Integer> docIdsDecompressed = new ArrayList<>();
    protected ArrayList<Integer> freqsDecompressed = new ArrayList<>();
    protected int currentPostingCounter;
    protected int howManyRecords;
    protected int nextRecordIndex;

    protected Posting currentPosting;

    protected static String docIdsPath = ConfigLoader.getProperty("blocks.invertedindex.docIdFilePath") + ConfigLoader.getProperty("blocks.merged.invertedIndex.path");
    protected static String freqsPath = ConfigLoader.getProperty("blocks.invertedindex.freqFilePath") + ConfigLoader.getProperty("blocks.merged.invertedIndex.path");
    protected static String skipBlockPath = ConfigLoader.getProperty("blocks.merged.skipBlocks.path");
    protected BinaryFileManager docIdsBinaryFileManager;
    protected BinaryFileManager freqsBinaryFileManager;

    @Override
    public void openList(VocabularyFileRecord vocabularyFileRecord) {

        this.docIdsBinaryFileManager = new BinaryFileManager(docIdsPath, MODE.READ, new DeltaCompressor());
        this.freqsBinaryFileManager = new BinaryFileManager(freqsPath, MODE.READ, new UnaryCompressor());

        try {
            SkipBlockBlockManager sbm = new SkipBlockBlockManager(skipBlockPath, MODE.READ);
            //the loop handle the case in which a posting list is split among two or more skip blocks
            for (int i = 0; i < vocabularyFileRecord.getHowManySkipBlocks(); i++) {
                SkipBlock sb = sbm.readRowAt(vocabularyFileRecord.getOffset() + i*SkipBlock.SKIP_BLOCK_ENTRY_SIZE);
                // from int[] to ArrayList<Integer>
                docIdsDecompressed.addAll( Arrays.stream(docIdsBinaryFileManager.readIntArray(sb.getDocIdByteSize(), sb.getDocIdFileOffset(), sb.getHowManyPostings())).boxed().toList() );
                freqsDecompressed.addAll( Arrays.stream(freqsBinaryFileManager.readIntArray(sb.getFreqByteSize(), sb.getFreqFileOffset(), sb.getHowManyPostings())).boxed().toList() ) ;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        this.howManyRecords = vocabularyFileRecord.getDf();
        reset();
    }

    @Override
    public Posting next() {
        if( ! this.hasNext() ){
            //in this case I have already read all the Postings for this posting list,
            // then we return null
            return null;
        }

        int docId = docIdsDecompressed.get(currentPostingCounter);
        int freq = freqsDecompressed.get(currentPostingCounter);

        this.nextRecordIndex += 1;
        this.currentPostingCounter += 1;

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
        return null;
    }

    /**
     * resets the iterator to the first element of the ids and freqs of the posting list
     */
    protected void reset(){
        /*try {

            docIdsBinaryFileManager.seek(offsetIITwoFilesArray.get(0).getBytesOffsetDocId());
            freqsBinaryFileManager.seek(offsetIITwoFilesArray.get(0).getBytesOffsetFreq());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        this.currentPostingCounter = 0;
        this.nextRecordIndex = 0;
        this.currentPosting = null;
    }

    @Override
    public void closeList() {
        this.freqsBinaryFileManager.close();
        this.freqsBinaryFileManager.close();
    }
}
