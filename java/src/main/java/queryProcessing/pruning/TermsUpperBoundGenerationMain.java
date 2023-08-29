package queryProcessing.pruning;

import java.sql.Timestamp;

import indexing.IndexerMain;

public class TermsUpperBoundGenerationMain {
    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        System.out.println(new Timestamp(begin) + "\tStarting...");
        IndexerMain.upperBoundOnly(args);
        long end = System.currentTimeMillis();
        System.out.println(new Timestamp(end) + "\tFinished");
        System.out.println("Elapsed time for terms upper bound generation: " + (end - begin));

    }
}
