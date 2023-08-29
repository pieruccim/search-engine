package queryProcessing.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import common.bean.VocabularyFileRecord;
import common.manager.block.VocabularyBlockManager.OffsetType;
import common.utils.LRUCache;
import config.ConfigLoader;
import javafx.util.Pair;



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

    private static final int howManyThreads = 10;

    private static ExecutorService executor = Executors.newFixedThreadPool(howManyThreads);

    private static ArrayList<Pair<VocabularyFileRecord, Future<PostingListIterator>>> futureList = new ArrayList<Pair<VocabularyFileRecord, Future<PostingListIterator>>>();

    /**
     * opens all the iterators associated with the given vocabularyfilerecords in a concurrent way
     * @param records
     * @param retList the arraylist where the opened / retrieved iterators will be put
     */
    public static void openIterators(List<VocabularyFileRecord> records, ArrayList<Pair<VocabularyFileRecord, PostingListIterator>> retList){
        
        if(printedInfos == false){
            printedInfos = printInfos();
        }
        
        futureList.clear();

        for (VocabularyFileRecord vocabularyFileRecord : records) {
            futureList.add( new Pair<VocabularyFileRecord, Future<PostingListIterator>>(vocabularyFileRecord, executor.submit(() -> {return openIterator(vocabularyFileRecord);})));
        }

        for (Pair<VocabularyFileRecord, Future<PostingListIterator>> record : futureList) {
            PostingListIterator pl = null;
            try {
				if(record.getValue() != null)
                    pl = record.getValue().get();

			} catch (InterruptedException e) {
				//e.printStackTrace();
                pl = null;
			} catch (ExecutionException e) {
				//e.printStackTrace();
                pl = null;
			}
            if(pl == null){
                if(record.getValue() != null)
                    record.getValue().cancel(true);
                System.out.println("Loading iterator in main thread for the term: " + record.getKey().getTerm());
                pl = openIterator(record.getKey());
            }
            retList.add(new Pair<VocabularyFileRecord, PostingListIterator>(record.getKey(), pl));
        }
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
        if(executor != null){
            executor.shutdown();
        }

    }
}
