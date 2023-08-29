package queryProcessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import common.bean.CollectionStatistics;
import common.bean.DocumentIndexFileRecord;
import common.bean.VocabularyFileRecord;
import common.bean.VocabularyFileRecordUB;
import common.manager.CollectionStatisticsManager;
import common.manager.block.DocumentIndexBlockManager;
import config.ConfigLoader;
import preprocessing.Preprocessor;
import queryProcessing.DocumentProcessor.*;
import queryProcessing.manager.VocabularyManager;
import queryProcessing.scoring.BM25;
import queryProcessing.scoring.ScoreFunction;
import queryProcessing.scoring.TFIDF;


public class QueryProcessor {

    public enum ScoringFunction{
        BM25,
        TFIDF
    };

    public enum QueryType{
        CONJUNCTIVE,
        DISJUNCTIVE
    };

    public enum DocumentProcessorType{
        DAAT,
        TAAT,
        MAXSCORE
    };

    private QueryType queryType;
    private boolean stopwordsRemoval;
    private boolean wordStemming;
    private int nResults;

    private CollectionStatisticsManager collectionStatisticsManager = new CollectionStatisticsManager(ConfigLoader.getProperty("collectionStatistics.filePath"));
    private DocumentProcessor documentProcessor;
    private ScoreFunction scoreFunction;

    private int numDocs;

    private HashMap<String, VocabularyFileRecord> vocabulary;
    private HashMap<String, VocabularyFileRecordUB> vocabularyUB;

    private DocumentProcessorType documentProcessorType;

    public QueryProcessor(int nResults, ScoringFunction scoringFunctionType, QueryType queryType, DocumentProcessorType documentProcessorType, Boolean stopwordsRemoval, Boolean wordStemming){
        this.nResults = nResults;
        this.numDocs = getNumDocs();
        System.out.println("Using numDocs: " + numDocs);
        System.out.println("Using DocumentProcessorType: " + documentProcessorType.toString());
        System.out.println("Using queryType: " + queryType.toString());
        System.out.println("Using scoringFunctionType: " + scoringFunctionType.toString());

        this.documentProcessorType = documentProcessorType;

        if(documentProcessorType == DocumentProcessorType.DAAT) {
            this.documentProcessor = new DAAT();
        }
        else if (documentProcessorType == DocumentProcessorType.TAAT){
            this.documentProcessor = new TAAT();
        }else if(documentProcessorType == DocumentProcessorType.MAXSCORE){
            this.documentProcessor = new MaxScore();
        }
        else{
            throw new UnsupportedOperationException("unsupported Document Processor type");
        }

        switch (scoringFunctionType) {
            case TFIDF:
                this.scoreFunction = new TFIDF(numDocs);
                break;

            case BM25:
                this.scoreFunction = new BM25(numDocs, this.getAvgDocLength(), this.loadDocumentIndexLengthInformation());
                break;
        
            default:
                throw new UnsupportedOperationException("undefined scoring function");
        }

        this.queryType = queryType;
        this.stopwordsRemoval = stopwordsRemoval;
        this.wordStemming = wordStemming;

        Preprocessor.setPerformStemming(this.wordStemming);
        Preprocessor.setRemoveStopwords(this.stopwordsRemoval);

        if(documentProcessorType == DocumentProcessorType.MAXSCORE){
            this.vocabularyUB = VocabularyManager.loadVocabularyWithUBs(scoringFunctionType);
        }else{
            this.vocabulary = VocabularyManager.loadVocabulary();
        }
        //System.out.println("Loaded the vocabulary made of " + vocabulary.size() + " records");

    }

    private int getNumDocs() {
        CollectionStatistics collectionStatistics;
        try {
            collectionStatistics = collectionStatisticsManager.readCollectionStatistics();
        } catch (IOException e) {
            System.out.println("Unable to load Collection Statistics");
            throw new RuntimeException(e);
        }

        return collectionStatistics.getTotalDocuments();
    }

    private double getAvgDocLength() {
        CollectionStatistics collectionStatistics;
        try {
            collectionStatistics = collectionStatisticsManager.readCollectionStatistics();
        } catch (IOException e) {
            System.out.println("Unable to load Collection Statistics");
            throw new RuntimeException(e);
        }

        return collectionStatistics.getAverageDocumentLength();
    }
    /**
     * 
     * @return an int[] whose length is the same as the number of documents in the collection, 
     * the array element at index X contains the document length for the document of docID X
     */
    private int[] loadDocumentIndexLengthInformation(){
        DocumentIndexBlockManager documentIndexBlockManager = null;
        
        try {
            documentIndexBlockManager = DocumentIndexBlockManager.getMergedFileManager();
        } catch (IOException e) {
            e.printStackTrace();
        }
       
        int [] documentIndexLengthsInformation = new int[this.getNumDocs()];
        int i = 0;
        int howMany = -1;
        
        while (howMany != 0) {
            howMany = documentIndexBlockManager.readDocumentLenghtsList(documentIndexLengthsInformation, i);
            i += howMany;
        }
        

        if(i != this.getNumDocs()){
            System.out.println("[loadDocumentIndexLengthInformation] DEBUG: i: " + i + " \t numDocs: " + this.getNumDocs());
        }

        //for (int id = 1000; id < 1100; id+=10) {
        //    System.out.println("document lenth for docid: " + id + " -> " + documentIndexLengthsInformation[id]);
        //}
        

        return documentIndexLengthsInformation;
    }


    public List<DocumentScore> processQuery(String query){
        return processQuery(query, 0);
    }
    // it should return an hashmap<String, PostingListIterator> for each query term
    public List<DocumentScore> processQuery(String query, long begin_time){

        String[] queryTerms = Preprocessor.processText(query, false);
        if(begin_time != 0){
            System.out.println("elapsed time for query preprocessing: " + ( System.currentTimeMillis() - begin_time ) );
        }
        if(documentProcessorType != DocumentProcessorType.MAXSCORE){

            ArrayList<VocabularyFileRecord> queryRecords = new ArrayList<VocabularyFileRecord>();

            for (String term : queryTerms) {
                if(! vocabulary.containsKey(term)){
                    continue;
                }
                queryRecords.add(vocabulary.get(term));
            }

        if(begin_time != 0){
            System.out.println("elapsed time until queryRecords generation: " + ( System.currentTimeMillis() - begin_time ) );
        }

            return this.documentProcessor.scoreDocuments(queryRecords, this.scoreFunction, this.queryType, this.nResults);
        }else{

            ArrayList<VocabularyFileRecordUB> queryRecords = new ArrayList<VocabularyFileRecordUB>();

            for (String term : queryTerms) {
                if(! vocabularyUB.containsKey(term)){
                    continue;
                }
                queryRecords.add(vocabularyUB.get(term));
            }

            if(begin_time != 0){
                System.out.println("elapsed time until queryRecords generation: " + ( System.currentTimeMillis() - begin_time ) );
            }

            return ((MaxScore) this.documentProcessor).scoreDocumentsUB(queryRecords, this.scoreFunction, this.queryType, this.nResults);

        }

    }
}
