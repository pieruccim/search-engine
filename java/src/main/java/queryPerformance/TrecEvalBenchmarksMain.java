package queryPerformance;

import javafx.util.Pair;
import preprocessing.Preprocessor;
import queryProcessing.DocumentProcessor.*;
import queryProcessing.QueryProcessor;
import queryProcessing.QueryProcessor.*;

import java.util.List;

public class TrecEvalBenchmarksMain {
    public static void main(String[] args) {

        //default parameters
        ScoringFunction scoringFunction = ScoringFunction.TFIDF;
        QueryType queryType = QueryType.DISJUNCTIVE;
        DocumentProcessorType documentProcessorType = DocumentProcessorType.DAAT;
        int nResults = 10;
        boolean stopWordRemoval = true;
        boolean wordStemming = true;

        for (int i = 0; i < args.length; i++) {
            if(args[i].equals("--results") && (i+1) < args.length){
                int tmp = Integer.parseInt(args[i+1]);
                if(tmp > 0){    // && tmp < MAX_THRESHOLD
                    nResults = tmp;
                }else{
                    System.out.println("The specified results number " + tmp + " is not valid, using default value " + nResults);
                }
                i+=1;
                continue;
            }
            if(args[i].equals("--scoring") && (i+1) < args.length){
                ScoringFunction tmp = null;
                try {
                    tmp = ScoringFunction.valueOf(args[i+1]);
                } catch (IllegalArgumentException e) {
                    //System.out.println("IllegalArgument for scoring\tAccepted values: " + ScoringFunction.values().toString());
                }
                if(tmp != null){    // && tmp < MAX_THRESHOLD
                    scoringFunction = tmp;
                }else{
                    System.out.println("The specified scoring function " + args[i+1] + " is not valid, using default value " + scoringFunction);
                }
                i+=1;
                continue;
            }
            if(args[i].equals("--queryType") && (i+1) < args.length){
                QueryType tmp = null;
                try {
                    tmp = QueryType.valueOf(args[i+1]);
                } catch (IllegalArgumentException e) {
                    //System.out.println("IllegalArgument for QueryType\tAccepted values: " + QueryType.values().toString());
                }
                if(tmp != null){
                    queryType = tmp;
                }else{
                    System.out.println("The specified Query type " + args[i+1] + " is not valid, using default value " + queryType);
                }
                i+=1;
                continue;
            }
            if(args[i].equals("--processingType") && (i+1) < args.length){
                DocumentProcessorType tmp = null;
                try {
                    tmp = DocumentProcessorType.valueOf(args[i+1]);
                } catch (IllegalArgumentException e) {
                    //System.out.println("IllegalArgument for documentProcessorType\tAccepted values: " + DocumentProcessorType.values().toString());
                }
                if(tmp != null){
                    documentProcessorType = tmp;
                }else{
                    System.out.println("The specified Processor type " + args[i+1] + " is not valid, using default value " + documentProcessorType);
                }
                i+=1;
                continue;
            }
            if(args[i].equals("--stopWords") && (i+1) < args.length){
                boolean tmp;
                try {
                    tmp = Boolean.parseBoolean(args[i+1]);
                } catch (IllegalArgumentException e) {
                    tmp = true; //default to true if parsing fails
                }
                stopWordRemoval = tmp;
                i += 1;
                continue;
            }
            if(args[i].equals("--wordStemming") && (i+1) < args.length){
                boolean tmp;
                try {
                    tmp = Boolean.parseBoolean(args[i+1]);
                } catch (IllegalArgumentException e) {
                    tmp = true;
                }
                wordStemming = tmp;
                i += 1;
            }
        }

        QueryProcessor queryProcessor = new QueryProcessor(nResults, scoringFunction, queryType, documentProcessorType, stopWordRemoval, wordStemming);
        TrecEvalBenchmarks teb = new TrecEvalBenchmarks();

        //process queries from the tsv trec_eval file
        List<Pair<Integer, String>> queries = teb.readQueries();

        int c = 1;
        for (Pair<Integer, String> queryPair : queries) {
            System.out.println("Evaluating Query n." + c + " ...");
            int queryId = queryPair.getKey();
            String queryText = queryPair.getValue();

            List<DocumentScore> results = queryProcessor.processQuery(queryText);
            if(results.size()>0){
                teb.storeTrecEvalResult(queryId, results);
            }else{
                System.out.println("No relevant docs found for query " + c);
            }


            c += 1;
            /*if (c == 5) {
                teb.closeQueriesFileManager();
                teb.closeResultFileManager();
                System.out.println("Done saving queries scores");
                System.exit(0);
            }*/
        }

        teb.closeQueriesFileManager();
        teb.closeResultFileManager();
        System.out.println("Done saving queries scores");
    }
}