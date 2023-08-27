package queryProcessing.manager;

import common.bean.VocabularyFileRecord;
import common.manager.block.VocabularyBlockManager.OffsetType;
import common.utils.LRUCache;
import config.ConfigLoader;



public class PostingListIteratorFactory {

    protected static OffsetType offsetType = OffsetType.valueOf(ConfigLoader.getProperty("blocks.invertedindex.type"));

    private final static LRUCache<String, PostingListIterator> LRUcache = new LRUCache<String, PostingListIterator>(1000, (PostingListIterator p) -> {p.closeList(); return true;});

    public static PostingListIterator openIterator(VocabularyFileRecord vocabularyFileRecord){

        PostingListIterator postingListIterator = LRUcache.get(vocabularyFileRecord.getTerm());

        if(postingListIterator != null){
            postingListIterator.reset();
            return postingListIterator;
        }

        switch (PostingListIteratorFactory.offsetType) {
            case SINGLE_FILE:
                postingListIterator = new PostingListIteratorSingleFile();
                break;
            case TWO_FILES:
                postingListIterator = new PostingListIteratorTwoFile();
                break;
            default:
                throw new UnsupportedOperationException("Unimplemented OffsetType handling for " + PostingListIteratorFactory.offsetType);
        }

        postingListIterator.openList(vocabularyFileRecord);

        LRUcache.put(vocabularyFileRecord.getTerm(), postingListIterator);

        return postingListIterator;
    }

    /**
     * 
     */
    public static void close(){
        if(PostingListIteratorFactory.offsetType == OffsetType.TWO_FILES){
            PostingListIteratorTwoFile.shutdownThreads();
        }

        for( PostingListIterator posting : LRUcache.values()){
            posting.closeList();
        }

    }
}
