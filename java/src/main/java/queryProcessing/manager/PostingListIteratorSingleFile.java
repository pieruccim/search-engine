package queryProcessing.manager;

import java.io.IOException;
import java.util.Iterator;

import common.bean.OffsetIISingleFile;
import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import common.manager.file.BinaryFileManager;
import common.manager.file.FileManager.MODE;
import config.ConfigLoader;

public class PostingListIteratorSingleFile implements PostingListIterator{
    
    protected long startingOffset;
    protected int howManyRecords;
    protected int nextRecordIndex;

    protected Posting currentPosting;

    protected static String postingListPath = ConfigLoader.getProperty("blocks.invertedIndex.path") + ConfigLoader.getProperty("blocks.merged.invertedIndex.path");
    protected BinaryFileManager binaryFileManager;

    @Override
    public void openList(VocabularyFileRecord vocabularyFileRecord) {
        // the offset must be an instance of OffsetIISingleFile if this postingListIterator type was instantiated
        OffsetIISingleFile offsetIISingleFile = (OffsetIISingleFile) vocabularyFileRecord.getOffset();
        startingOffset = offsetIISingleFile.getBytesOffsetDocId();
        howManyRecords = vocabularyFileRecord.getDf();
        binaryFileManager = new BinaryFileManager(postingListPath, MODE.READ);
        this.reset();

    }

    /**
     * resets the iterator to the first element of the posting list
     */
    protected void reset(){
        try {
            binaryFileManager.seek(startingOffset);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.nextRecordIndex = 0;
        this.currentPosting = null;
    }
    @Override
    public boolean hasNext(){
        return this.nextRecordIndex < this.howManyRecords;
    }
    @Override
    public Posting getCurrentPosting(){
        return this.currentPosting;
    }

    @Override
    public Posting next() {
        if( ! this.hasNext() ){
            //in this case I have already read all the Postings for this posting list,
            // then we return null
            return null;
        }
        int docId, freq;
        try {
            docId = binaryFileManager.readInt();
            freq = binaryFileManager.readInt();
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
        this.nextRecordIndex += 1;
        return this.currentPosting = new Posting(docId, freq);
    }

    /**
     * it does not modifies the binaryFileManager offset from which it reads from
     * @return the posting at the given index for this iterator
     */
    private Posting readAt(int index){
        if(index >= this.howManyRecords || index < 0){
            return null;
        }
        int POSTING_SIZE = 8; //in bytes // fixed size in case of single file without compression
        long offsetOfPosting = this.startingOffset + index * POSTING_SIZE;
        long storedOffset;
        try {
            storedOffset = this.binaryFileManager.getCurrentPosition();
            this.binaryFileManager.seek(offsetOfPosting);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        int docId, freq;
        try {
            docId = binaryFileManager.readInt();
            freq = binaryFileManager.readInt();
            this.binaryFileManager.seek(storedOffset);
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return new Posting(docId, freq);
    }

    /**
     * it updates the binaryFileManager offset from which it reads from
     * ence the next call at next() method will return the Posting at index + 1 if any
     * - it also allows to move at previous positions with respect to the currentIndex
     * @return the posting at the given index for this iterator
     */
    private Posting moveAt(int index){
        if(index >= this.howManyRecords || index < 0){
            return null;
        }
        int POSTING_SIZE = 8; //in bytes // fixed size in case of single file without compression
        long offsetOfPosting = this.startingOffset + index * POSTING_SIZE;
        try {
            this.binaryFileManager.seek(offsetOfPosting);
            this.nextRecordIndex = index;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        int docId, freq;
        try {
            docId = binaryFileManager.readInt();
            freq = binaryFileManager.readInt();
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return new Posting(docId, freq);
    }

    @Override
    public Posting nextGEQ(long docId) {

        Posting currPosting = this.getCurrentPosting();

        if(currPosting!= null && currPosting.getDocid() >= docId){
            //return currPosting;
            return this.next();
        }

        Posting lastPosting = this.readAt(this.howManyRecords - 1);
        if(lastPosting.getDocid() < docId){
            //TODO: should the currentIndex and the file pointer be moved to the end of the posting list in this case?
            return null;
        }else if(lastPosting.getDocid() == docId){
            return this.moveAt(this.howManyRecords - 1);
        }
        //case in which the lastPosting docId is > than the requested docId
        //we can perform binary search to retrieve the first Posting GEQ
        int lowerBound = this.nextRecordIndex;
        int upperBound = this.howManyRecords - 1;

        int middle = lowerBound + ((upperBound - lowerBound) / 2);

        Posting middlePosting = this.readAt(middle);

        while(lowerBound != upperBound){
            
            if(middlePosting.getDocid() < docId){
                
                if(middle == lowerBound){
                    return this.moveAt(upperBound);
                }
                
                lowerBound = middle;
                middle = lowerBound + ((upperBound - lowerBound) / 2);
                
            }else if(middlePosting.getDocid() == docId){
                return this.moveAt(middle);
            }else{
                // when we have middlePosting.getDocid() > docId
                upperBound = middle;
                middle = lowerBound + ((upperBound - lowerBound) / 2);
            }
            middlePosting = this.readAt(middle);
        }


        if(middlePosting.getDocid() >= docId){
            return this.moveAt(middle);
        }else{
            //this.currentRecordIndex = this.howManyRecords - 1;
            return null;
        }
    }

    @Override
    public void closeList() {
        this.binaryFileManager.close();
    }

    
}
