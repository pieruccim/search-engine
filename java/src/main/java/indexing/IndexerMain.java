package indexing;


import java.io.IOException;
import java.sql.Timestamp;
import java.util.Scanner;

import common.bean.CollectionStatistics;
import common.manager.CollectionStatisticsManager;
import config.ConfigLoader;
import preprocessing.Preprocessor;
import queryProcessing.QueryProcessor.ScoringFunction;
import queryProcessing.pruning.TermsUpperBoundManager;

import static common.manager.file.FileManager.checkExistingOutputFiles;

public class IndexerMain {

    public static void main(String[] args) {

        long beginTime = System.currentTimeMillis();
        System.out.println(new Timestamp(System.currentTimeMillis()) + "\tStarting...");

        Scanner input = new Scanner(System.in);

        Preprocessor.setRemoveStopwords(ConfigLoader.getPropertyBool("preprocessing.remove.stopwords"));
        Preprocessor.setPerformStemming(ConfigLoader.getPropertyBool("preprocessing.enable.stemming"));

        //check empty output path
        try {
            checkExistingOutputFiles(input);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Indexer indexer = new Indexer();
        indexer.processCorpus();

        CollectionStatistics collectionStatistics = indexer.getCollectionStatistics();

        long processingDoneTime = System.currentTimeMillis();

        System.out.println(new Timestamp(System.currentTimeMillis()) + "\tCorpus processed\tElapsed time: " + (processingDoneTime-beginTime) + " millis");

        indexer.mergeDataStructures();

        long mergingDoneTime = System.currentTimeMillis();

        System.out.println(new Timestamp(System.currentTimeMillis()) + "\tMerging done\tElapsed time from begin: " + (mergingDoneTime-beginTime) + " millis");

        generateUpperBounds(collectionStatistics, input, true);

        input.close();
        
    }

    public static void upperBoundOnly(String[] args){
        CollectionStatistics collectionStatistics;
        try {
            CollectionStatisticsManager collectionStatisticsManager = new CollectionStatisticsManager(ConfigLoader.getProperty("collectionStatistics.filePath"));
            collectionStatistics = collectionStatisticsManager.readCollectionStatistics();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        Scanner input = new Scanner(System.in);
        generateUpperBounds(collectionStatistics, input, true);
        input.close();
    }

    private static void generateUpperBounds(CollectionStatistics collectionStatistics, Scanner input, boolean doGenerate){


        for (ScoringFunction scoringFunction : ScoringFunction.values()) {
            
            if(doGenerate == false){
                System.out.println("Do you want to compute the upper bounds for the scoring function: '" + scoringFunction + "'? (y/n)");
                String answer = input.nextLine();
                if( ! answer.toLowerCase().equals("y") ){
                    continue;
                }
            }

            System.out.println("Going to generate stats for scoring function "+ scoringFunction);

            TermsUpperBoundManager.generateUpperBounds(collectionStatistics, scoringFunction);
        }
        TermsUpperBoundManager.close();
    } 
}
