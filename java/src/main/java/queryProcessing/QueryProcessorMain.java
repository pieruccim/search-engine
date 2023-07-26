package queryProcessing;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import queryProcessing.DAAT.DocumentScore;
import queryProcessing.QueryProcessor.QueryType;
import queryProcessing.QueryProcessor.ScoringFunction;
import queryProcessing.scoring.ScoreFunction;

public class QueryProcessorMain {
    public static void main(String[] args) {
        QueryProcessor queryProcessor = new QueryProcessor(10, ScoringFunction.TFIDF, QueryType.DISJUNCTIVE, true, true);

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
    }
}
