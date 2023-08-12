package queryProcessing;

import java.util.List;
import java.util.Scanner;

import config.ConfigLoader;
import queryProcessing.DocumentProcessor.*;
import queryProcessing.QueryProcessor.QueryType;
import queryProcessing.QueryProcessor.ScoringFunction;
import queryProcessing.QueryProcessor.DocumentProcessorType;

public class QueryProcessorMain {
    private static ScoringFunction scoringFunction = ScoringFunction.TFIDF;
    private static QueryType queryType = QueryType.DISJUNCTIVE;
    private static DocumentProcessorType documentProcessorType = DocumentProcessorType.DAAT;
    private static int nResults = Integer.parseInt(ConfigLoader.getProperty("query.parameters.nResults"));
    private static boolean stopwordsRemoval = Boolean.parseBoolean(ConfigLoader.getProperty("preprocessing.remove.stopwords"));
    private static boolean wordStemming = Boolean.parseBoolean(ConfigLoader.getProperty("preprocessing.enable.stemming"));

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);

        while (true) {
            showDefaultParameters(scoringFunction, queryType, documentProcessorType, nResults, stopwordsRemoval, wordStemming);

            System.out.print("Do you want to perform a query or change parameters? (query/params/exit): ");
            String choice = sc.nextLine().trim().toLowerCase();

            if (choice.equals("exit")) {
                System.out.println("Closing...");
                break;
            }
            else if (choice.equals("params")) {
                scoringFunction = getScoringFunction(sc);
                queryType = getQueryType(sc);
                documentProcessorType = getDocumentProcessorType(sc);
                nResults = getNumberOfResults(sc);
                stopwordsRemoval = getStopwordsRemoval(sc);
                wordStemming = getWordStemming(sc);
            }
            else if (choice.equals("query")) {
                QueryProcessor queryProcessor = new QueryProcessor(nResults, scoringFunction, queryType,
                        documentProcessorType, stopwordsRemoval, wordStemming);

                System.out.print("Insert the new query: ");
                String query = sc.nextLine();

                long start = System.currentTimeMillis();
                List<DocumentScore> results = queryProcessor.processQuery(query);
                long end = System.currentTimeMillis();

                System.out.println("Elapsed Time in milliseconds: " + (end - start));
                for (DocumentScore documentScore : results) {
                    System.out.println("docID: " + documentScore.getDocId() + " | score: " + documentScore.getScore());
                }
            } else {
                System.out.println("Invalid choice. Please enter 'query', 'params', or 'exit'.");
            }
        }

        sc.close();
    }

    private static void showDefaultParameters(ScoringFunction scoringFunction, QueryType queryType,
                                              DocumentProcessorType documentProcessorType, int nResults,
                                              boolean stopwordsRemoval, boolean wordStemming) {
        System.out.println("\nCurrent Parameters:\n" +
                "Scoring Function: " + scoringFunction + "\n" +
                "Query Type: " + queryType + "\n" +
                "Document Processor Type: " + documentProcessorType + "\n" +
                "Number of Results: " + nResults + "\n" +
                "Stopword Removal: " + stopwordsRemoval + "\n" +
                "Word Stemming: " + wordStemming + "\n");
    }

    private static ScoringFunction getScoringFunction(Scanner sc) {
        System.out.print("Enter the Scoring Function (TFIDF/BM25): ");
        String choice = sc.nextLine().trim().toUpperCase();
        if (choice.equals("BM25")) {
            return ScoringFunction.BM25;
        } else if (choice.equals("TFIDF")) {
            return ScoringFunction.TFIDF;
        } else {
            System.out.println("Unrecognized Scoring Function. Using default: " + ScoringFunction.TFIDF);
            return ScoringFunction.TFIDF;
        }
    }

    private static QueryType getQueryType(Scanner sc) {
        System.out.print("Enter the Query Type (CONJUNCTIVE/DISJUNCTIVE): ");
        String choice = sc.nextLine().trim().toUpperCase();
        if (choice.equals("CONJUNCTIVE")) {
            return QueryType.CONJUNCTIVE;
        } else if (choice.equals("DISJUNCTIVE")) {
            return QueryType.DISJUNCTIVE;
        } else {
            System.out.println("Unrecognized Query Type. Using default: " + QueryType.DISJUNCTIVE);
            return QueryType.DISJUNCTIVE;
        }
    }

    private static DocumentProcessorType getDocumentProcessorType(Scanner sc) {
        System.out.print("Enter the Document Processor Type (DAAT/TAAT): ");
        String choice = sc.nextLine().trim().toUpperCase();
        if (choice.equals("DAAT")) {
            return DocumentProcessorType.DAAT;
        } else if (choice.equals("TAAT")) {
            return DocumentProcessorType.TAAT;
        } else {
            System.out.println("Unrecognized Document Processor Type. Using default: " + DocumentProcessorType.DAAT);
            return DocumentProcessorType.DAAT;
        }
    }

    private static int getNumberOfResults(Scanner sc) {
        while (true) {
            try {
                System.out.print("Enter the number of results: ");
                return Integer.parseInt(sc.nextLine().trim());
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a valid integer.");
            }
        }
    }

    private static boolean getStopwordsRemoval(Scanner sc) {
        System.out.print("Enable Stopword Removal? (yes/no): ");
        String choice = sc.nextLine().trim().toLowerCase();
        if (!choice.equals("yes") && !choice.equals("no")) {
            System.out.println("Unrecognized choice. Using default: yes");
            return true; //default yes
        }
        return choice.equals("yes");
    }

    private static boolean getWordStemming(Scanner sc) {
        System.out.print("Enable Word Stemming? (yes/no): ");
        String choice = sc.nextLine().trim().toLowerCase();
        if (!choice.equals("yes") && !choice.equals("no")) {
            System.out.println("Unrecognized choice. Using default: yes");
            return true;
        }
        return choice.equals("yes");
    }

}
