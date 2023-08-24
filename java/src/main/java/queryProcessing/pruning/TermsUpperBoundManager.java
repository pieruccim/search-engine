package queryProcessing.pruning;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import common.bean.CollectionStatistics;
import common.bean.DocumentIndexFileRecord;
import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import common.manager.block.DocumentIndexBlockManager;
import common.manager.file.BinaryFileManager;
import common.manager.file.FileManager.MODE;
import config.ConfigLoader;
import queryProcessing.QueryProcessor.ScoringFunction;
import queryProcessing.manager.PostingListIterator;
import queryProcessing.manager.PostingListIteratorFactory;
import queryProcessing.scoring.BM25;
import queryProcessing.scoring.ScoreFunction;
import queryProcessing.scoring.TFIDF;

public class TermsUpperBoundManager {

    protected CollectionStatistics collectionStatistics;
    protected ScoreFunction scoreFunction;
    protected HashMap<String, VocabularyFileRecord> vocabulary;

    protected final static String outputFileDirectory = ConfigLoader.getProperty("data.output.upperBounds.path");
    protected final String outputFilePath;

    protected BinaryFileManager binaryFileManager;

    /**
     * You have to instantiate a TermsUpperBoundManager in case of terms upper bound generation,
     * if you want to load the list of upper bounds from file, use the static method instead
     * @param collectionStatistics
     * @param scoringFunctionType
     * @param vocabulary
     */
    public TermsUpperBoundManager(CollectionStatistics collectionStatistics, ScoringFunction scoringFunctionType, HashMap<String, VocabularyFileRecord> vocabulary){
        this.collectionStatistics = collectionStatistics;
        this.vocabulary = vocabulary;

        switch (scoringFunctionType) {
            case TFIDF:
                this.outputFilePath = TermsUpperBoundManager.outputFileDirectory + "tfidfUpperBounds.binary";
                this.handleFileOpening();
                this.scoreFunction = new TFIDF(collectionStatistics.getTotalDocuments());
                break;

            case BM25:
                this.outputFilePath = TermsUpperBoundManager.outputFileDirectory + "BM25UpperBounds.binary";
                this.handleFileOpening();
                this.scoreFunction = new BM25(collectionStatistics.getTotalDocuments(), collectionStatistics.getAverageDocumentLength(), this.loadDocumentIndexLengthInformation(), 1.2, 0.75);
                break;
        
            default:
                throw new UnsupportedOperationException("undefined scoring function");
        }
        System.out.println("Starting generation of upper bounds...");
        this.generate();
        System.out.println("Done!");

        this.binaryFileManager.close();

    }
    /**
     * effectively generates the terms upper bounds
     */
    protected void generate(){
        
        for (VocabularyFileRecord vocabularyRecord : this.vocabulary.values()) {
            Posting current = null;
            double termUpperBound = -1;
            // for each term, we open its iterator
            PostingListIterator iterator = PostingListIteratorFactory.openIterator(vocabularyRecord);

            while (iterator.hasNext()) {
                current = iterator.next();
                termUpperBound = Math.max(termUpperBound, this.scoreFunction.documentWeight(vocabularyRecord, current) );
            }
            //here, we have iterated over the whole posting list for that term
            try {
                binaryFileManager.writeDouble(termUpperBound);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("[TermsUpperBoundGenerator] execution aborted");
                return;
            }
        }
    }

    /**
     * function that checks if a file already exists with that file name, in that case asks to the user if the file has to be overwritten
     * then it opens the binaryFileManager for that file
     */
    private void handleFileOpening(){
        if( ! ( new File(TermsUpperBoundManager.outputFileDirectory)).exists() ){
            try {
                Files.createDirectories(Paths.get(TermsUpperBoundManager.outputFileDirectory));
            } catch (IOException e) {
                System.out.println("[TermsUpperBoundGenerator.handleFileOpening]: Could not create directories for the path: " + outputFileDirectory);
                e.printStackTrace();
            }
        }
        this.binaryFileManager = new BinaryFileManager(this.outputFilePath, MODE.WRITE);
    }


    public static ArrayList<Double> loadUpperBoundsFromFile(ScoringFunction scoringFunctionType){
        
        ArrayList<Double> ret = new ArrayList<Double>();
        
        String inputFilePath;
        BinaryFileManager binaryFileManager;

        switch (scoringFunctionType) {
            case TFIDF:
                inputFilePath = TermsUpperBoundManager.outputFileDirectory + "tfidfUpperBounds.binary";
                break;

            case BM25:
                inputFilePath = TermsUpperBoundManager.outputFileDirectory + "BM25UpperBounds.binary";
                break;
        
            default:
                throw new UnsupportedOperationException("undefined scoring function");
        }
        binaryFileManager = new BinaryFileManager(inputFilePath, MODE.READ);

        double value;

        while(true){
            try {
                value = binaryFileManager.readDouble();
                ret.add(value);
            } catch (EOFException e) {
                break;
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return ret;
    }

    /**
     * 
     * @return an int[] whose length is the same as the number of documents in the collection, 
     * the array element at index X contains the document length for the document of docID X
     */
    private int[] loadDocumentIndexLengthInformation(){
        DocumentIndexBlockManager documentIndexBlockManager = null;
        
        try {
            documentIndexBlockManager = DocumentIndexBlockManager.getMergedFileManager();
        } catch (IOException e) {
            e.printStackTrace();
        }
        DocumentIndexFileRecord documentIndexFileRecord = null;
        
        int [] documentIndexLengthsInformation = new int[collectionStatistics.getTotalDocuments()];
        int i = 0;
        try {
            while(( documentIndexFileRecord = documentIndexBlockManager.readRow()) != null){
                documentIndexLengthsInformation[i] = documentIndexFileRecord.getLen();
                i+=1;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(i != collectionStatistics.getTotalDocuments()){
            System.out.println("[loadDocumentIndexLengthInformation] DEBUG: i: " + i + " \t numDocs: " + collectionStatistics.getTotalDocuments());
        }
        return documentIndexLengthsInformation;
    }
}
