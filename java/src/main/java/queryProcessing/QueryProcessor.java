package queryProcessing;

import java.util.ArrayList;

public class QueryProcessor {

    public enum ScoringFunction{
        BM25,
        TFIDF
    };

    public enum QueryType{
        CONJUNCTIVE,
        DISJUNCTIVE
    };

    private ScoringFunction scoringFunction;
    private QueryType queryType;
    private boolean stopwordsRemoval;
    private boolean wordStemming;

    public QueryProcessor(int nResults, ScoringFunction scoringFunction, QueryType queryType, Boolean stopwordsRemoval, Boolean wordStemming){
        //TODO: String documentProcessor type (e.g. DAAT)
        this.scoringFunction = scoringFunction;
        this.queryType = queryType;
        this.stopwordsRemoval = stopwordsRemoval;
        this.wordStemming = wordStemming;


    }

    // it should return an hashmap<String, PostingListIterator> for each query term
    //public static ArrayList<String> processQuery(String query){
    //    ArrayList<String> ret = new ArrayList<>();
    //    return ret;
    //}
}
