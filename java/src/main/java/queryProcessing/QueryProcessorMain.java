package queryProcessing;

import java.util.List;
import java.util.Scanner;

import config.ConfigLoader;
import queryProcessing.DocumentProcessor.*;
import queryProcessing.QueryProcessor.QueryType;
import queryProcessing.QueryProcessor.ScoringFunction;
import queryProcessing.QueryProcessor.DocumentProcessorType;

public class QueryProcessorMain {
    public static void main(String[] args) {

        ScoringFunction scoringFunction = ScoringFunction.TFIDF;
        QueryType queryType = QueryType.DISJUNCTIVE;
        DocumentProcessorType documentProcessorType = DocumentProcessorType.MAXSCORE;
        int nResults = Integer.parseInt(ConfigLoader.getProperty("query.parameters.nResults"));
        boolean stopWordRemoval = Boolean.parseBoolean(ConfigLoader.getProperty("preprocessing.remove.stopwords"));
        boolean wordStemming = Boolean.parseBoolean(ConfigLoader.getProperty("preprocessing.enable.stemming"));

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

        Scanner sc = new Scanner(System.in);

        while(true){
            System.out.print("Insert the new query: ");
            String query = sc.nextLine();
            if(query.equals("!exit")){
                System.out.println("Closing...");
                break;
            }
            long start = System.currentTimeMillis();
            List<DocumentScore> results = queryProcessor.processQuery(query);
            long end = System.currentTimeMillis();
            System.out.println("Elapsed Time in milliseconds: "+ (end-start));
            for (DocumentScore documentScore : results) {
                System.out.println("docID: " + documentScore.getDocId() + " | score: " + documentScore.getScore());
            }
        }
        sc.close();
    }
}
