package queryProcessing.manager;

import java.util.Iterator;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;

public interface PostingListIterator extends Iterator<Posting>{

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
     * 
     * @return true if the iterator has not reached the end, else returns true
     */
    public boolean hasNext();
    /**
     * 
     * @return the current posting or null if no posting were read still
     */
    public Posting getCurrentPosting();
    
    /**
     * moves the iterator towards the end and returns the first Posting whose docId is greater or equal to the specified one
     * @param docId the docId to iterate to
     * @return Posting the first posting list whose docId is >= to the given one, if no Posting is found, returns null
     */
    public Posting nextGEQ(long docId);

    /**
     * 
     */
    public void closeList();
}
