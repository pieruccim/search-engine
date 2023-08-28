package queryProcessing.manager;

import common.bean.VocabularyFileRecord;
import common.manager.block.VocabularyBlockManager.OffsetType;
import common.utils.LRUCache;
import config.ConfigLoader;



public class PostingListIteratorFactory {

    protected static OffsetType offsetType = OffsetType.valueOf(ConfigLoader.getProperty("blocks.invertedindex.type"));
    protected static final int cacheSize = ConfigLoader.getIntProperty("performance.iteratorFactory.cache.size");
    protected static final boolean useCache = ConfigLoader.getPropertyBool("performance.iteratorFactory.cache.enabled");

    private final static LRUCache<String, PostingListIterator> LRUcache = new LRUCache<String, PostingListIterator>(cacheSize, (PostingListIterator p) -> {p.closeList(); return true;});

    private static boolean printedInfos = false;

    private static boolean printInfos(){
        System.out.println("PostingListIteratorFactory use cache: " + ((useCache) ? (true + "\tcache size: " + cacheSize) : false ) );
        System.out.println("PostingListIteratorTwoFile use threads: " + PostingListIteratorTwoFile.useThreads );
        System.out.println("PostingListIteratorTwoFile use PostingListBlocks cache: " + ((PostingListIteratorTwoFile.useCache) ? (true + "\tcache size: " + PostingListIteratorTwoFile.cacheSize) : false ) );
        return true;
    }

    public static PostingListIterator openIterator(VocabularyFileRecord vocabularyFileRecord){

        if(printedInfos == false){
            printedInfos = printInfos();
        }

        PostingListIterator postingListIterator = useCache ? LRUcache.get(vocabularyFileRecord.getTerm()) : null;

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

        if(useCache)
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

        if(useCache){
            for( PostingListIterator posting : LRUcache.values()){
                posting.closeList();
            }
        }

    }
}
