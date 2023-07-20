package queryProcessing.manager;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;

public interface PostingListIterator {

    /**
     * 
     * @param vocabularyFileRecord relative to the term of the posting list iterator to load
     */
    public void openList(VocabularyFileRecord vocabularyFileRecord);

    /**
     * @return returns the next Posting in the iterator if any, else returns null
     */
    public Posting next();
    
    /**
     * moves the iterator towards the end and returns the first Posting whose docId is greater or equal to the specified one
     * @param docId the docId to iterate to
     * @return Posting the first posting list whose docId is >= to the given one, if no Posting is found, returns null
     */
    public Posting nextGEQ(long docId);

    /**
     * 
     * @return true if the iterator over the posting was closed correctly, else false
     */
    public boolean closeList();
}
