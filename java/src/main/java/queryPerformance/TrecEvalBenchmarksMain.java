package queryPerformance;

import javafx.util.Pair;
import preprocessing.Preprocessor;
import queryProcessing.DocumentProcessor.*;
import queryProcessing.QueryProcessor;
import queryProcessing.QueryProcessor.*;

import java.util.ArrayList;
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
        teb.closeQueriesFileManager();

        double sumQueryProcessingTimes = 0;
        List<Double> queryProcessingTimes = new ArrayList<>();

        int c = 1;
        for (Pair<Integer, String> queryPair : queries) {
            String output = "Evaluating Query n." + c + "/" + queries.size() + " ...";
            System.out.print(output + "\r");
            int queryId = queryPair.getKey();
            String queryText = queryPair.getValue();

            long start = System.nanoTime();
            List<DocumentScore> results = queryProcessor.processQuery(queryText);
            long end = System.nanoTime();
            double elapsed = (end - start) / 1000000;
            sumQueryProcessingTimes += elapsed;
            queryProcessingTimes.add(elapsed);

            if (results.size() > 0) {
                teb.storeTrecEvalResult(queryId, results);
            } else {
                System.out.println("No relevant docs found for query " + c);
            }

            c += 1;
            /*if (c == 100) {
                break;
            }*/
        }

        double averageTime = sumQueryProcessingTimes / queryProcessingTimes.size();
        double stdDeviation = computeStdDev(queryProcessingTimes, averageTime);

        teb.closeResultFileManager();
        System.out.println("Done saving queries scores");
        System.out.println("Average query time: " + String.format("%.2f", averageTime) + "ms ± " + String.format("%.1f", stdDeviation));
    }

    public static double computeStdDev(List<Double> queryProcessingTimes, double averageTime) {
        double sumSquaredDifferences = 0;
        for (Double time : queryProcessingTimes) {
            double difference = time - averageTime;
            sumSquaredDifferences += difference * difference;
        }
        return Math.sqrt(sumSquaredDifferences / queryProcessingTimes.size());
    }
}