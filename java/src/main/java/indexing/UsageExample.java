package indexing;

import javafx.util.Pair;
import preprocessing.Preprocessor;

public class UsageExample {

    public static void main(String[] args) {

        Indexer indexer = new Indexer();
        indexer.processCorpus();
        indexer.mergeDocumentIndex();

    }
}
