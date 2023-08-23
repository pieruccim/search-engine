package queryPerformance;

import common.manager.file.FileManager.*;
import common.manager.file.TextualFileManager;
import config.ConfigLoader;
import javafx.util.Pair;
import preprocessing.Preprocessor;
import queryProcessing.DocumentProcessor.*;

import java.util.ArrayList;
import java.util.List;

public class TrecEvalBenchmarks {

    private static final String fixed = "Q0";
    private static final String runId = "STANDARD";
    private static final String queriesCharset = ConfigLoader.getProperty("performance.queries.charset");
    private static final String queriesPath = ConfigLoader.getProperty("performance.queries.path");
    private static final String resultsPath = ConfigLoader.getProperty("performance.results.path");
    protected TextualFileManager queriesFileManager;
    protected TextualFileManager resultsFileManager;

    public TrecEvalBenchmarks(){
        this.queriesFileManager = new TextualFileManager(queriesPath, MODE.READ, queriesCharset);
        this.resultsFileManager = new TextualFileManager(resultsPath, MODE.WRITE);
    }

    /**
     * The method is responsible for reading the queries from the benchmark file 'queries.dev.tsv'
     * @return a list of pairs queryId-queryText
     */
    public List<Pair<Integer, String>> readQueries() {
        List<Pair<Integer, String>> queries = new ArrayList<>();

        String query;
        while ((query = queriesFileManager.readLine()) != null) {
            Pair<Integer, String> queryPair = Preprocessor.parseLine(query);
            queries.add(queryPair);
        }

        return queries;
    }

    /**
     * The method is responsible for writing on file each query read from the benchmark file along with its document score
     * @param queryId is the first field in each row inside queries.dev.tsv file
     * @param results is a list of DocumentScore elements containing the pair docId-score for the current query
     */
    protected void storeTrecEvalResult(int queryId, List<DocumentScore> results) {
        if(results.size()==0){
            return;
        }

        int i = 1;

        //writing results in trec_eval suitable format
        for (DocumentScore documentScore : results) {
            String resultLine = queryId + "\t" + fixed + "\t" + documentScore.getDocId() +
                    "\t" + i + "\t" + documentScore.getScore() + "\t" + runId;
            resultsFileManager.writeLine(resultLine);
            i++;
        }
    }

    protected void closeResultFileManager(){
        this.resultsFileManager.close();
    }

    protected void closeQueriesFileManager(){
        this.queriesFileManager.close();
    }
}