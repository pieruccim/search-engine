package queryProcessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import common.bean.VocabularyFileRecord;
import common.manager.block.VocabularyBlockManager;
import preprocessing.Preprocessor;
import queryProcessing.DocumentProcessor.*;
import queryProcessing.scoring.ScoreFunction;
import queryProcessing.scoring.TFIDF;

import javax.print.Doc;

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
        TAAT
    };

    private QueryType queryType;
    private boolean stopwordsRemoval;
    private boolean wordStemming;
    private int nResults;

    private DocumentProcessor documentProcessor;
    private ScoreFunction scoreFunction;

    private int numDocs = 20000;

    private HashMap<String, VocabularyFileRecord> vocabulary;

    public QueryProcessor(int nResults, ScoringFunction scoringFunctionType, QueryType queryType, DocumentProcessorType documentProcessorType, Boolean stopwordsRemoval, Boolean wordStemming){
        this.nResults = nResults;
        System.out.println("USING HARD CODED numDocs value at " + numDocs);

        if(documentProcessorType == DocumentProcessorType.DAAT) {
            this.documentProcessor = new DAAT();
        }
        else if (documentProcessorType == DocumentProcessorType.TAAT){
            this.documentProcessor = new TAAT();
        }
        else{
            throw new UnsupportedOperationException("unsupported Document Processor type");
        }

        switch (scoringFunctionType) {
            case TFIDF:
                this.scoreFunction = new TFIDF(numDocs);
                break;
        
            default:
                throw new UnsupportedOperationException("undefined scoring function");
        }

        this.queryType = queryType;
        this.stopwordsRemoval = stopwordsRemoval;
        this.wordStemming = wordStemming;

        Preprocessor.setPerformStemming(this.wordStemming);
        Preprocessor.setRemoveStopwords(this.stopwordsRemoval);

        VocabularyBlockManager vocabularyBlockManager = null;
        
        try {
            vocabularyBlockManager = VocabularyBlockManager.getMergedFileManager();
        } catch (IOException e) {
            e.printStackTrace();
        }
        VocabularyFileRecord vocabularyFileRecord = null;
        this.vocabulary = new HashMap<String, VocabularyFileRecord>();
        try {
            while(( vocabularyFileRecord = vocabularyBlockManager.readRow()) != null){
                vocabulary.put(vocabularyFileRecord.getTerm(), vocabularyFileRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Loaded the vocabulary made of " + vocabulary.size() + " records");

    }

    // it should return an hashmap<String, PostingListIterator> for each query term
    public List<DocumentScore> processQuery(String query){

        String[] queryTerms = Preprocessor.processText(query, false);

        ArrayList<VocabularyFileRecord> queryRecords = new ArrayList<VocabularyFileRecord>();

        for (String term : queryTerms) {
            if(! vocabulary.containsKey(term)){
                continue;
            }
            queryRecords.add(vocabulary.get(term));
        }

        return this.documentProcessor.scoreDocuments(queryRecords, this.scoreFunction, this.queryType, this.nResults);


    }
}
