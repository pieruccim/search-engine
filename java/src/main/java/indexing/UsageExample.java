package indexing;


import java.sql.Timestamp;
import config.ConfigLoader;
import javafx.util.Pair;
import preprocessing.Preprocessor;

public class UsageExample {

    public static void main(String[] args) {

        long beginTime = System.currentTimeMillis();
        System.out.println(new Timestamp(System.currentTimeMillis()) + "\tStarting...");

        Preprocessor.setRemoveStopwords(true);
        Preprocessor.setPerformStemming(true);

        Indexer indexer = new Indexer();
        indexer.processCorpus();

        long processingDoneTime = System.currentTimeMillis();

        System.out.println(new Timestamp(System.currentTimeMillis()) + "\tCorpus processed\tElapsed time: " + (processingDoneTime-beginTime) + " millis");

        indexer.mergeDataStructures();

        long mergingDoneTime = System.currentTimeMillis();

        System.out.println(new Timestamp(System.currentTimeMillis()) + "\tMerging done\tElapsed time from begin: " + (mergingDoneTime-beginTime) + " millis");

    }
}
