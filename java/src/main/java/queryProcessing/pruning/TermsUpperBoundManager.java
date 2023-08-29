package queryProcessing.pruning;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import common.bean.CollectionStatistics;
import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import common.manager.block.DocumentIndexBlockManager;
import common.manager.block.VocabularyBlockManager;
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

    protected final static String outputFileDirectory = ConfigLoader.getProperty("data.output.upperBounds.path");
    

    /**
     * You have to instantiate a TermsUpperBoundManager in case of terms upper bound generation,
     * if you want to load the list of upper bounds from file, use the static method instead
     * @param collectionStatistics
     * @param scoringFunctionType
     * @param vocabulary
     */
    static public void generateUpperBounds(CollectionStatistics collectionStatistics, ScoringFunction scoringFunctionType){

        ScoreFunction scoreFunction;
        VocabularyBlockManager vocabularyBlockManager;
        final String outputFilePath;
        BinaryFileManager binaryFileManager;
        
        try {
            vocabularyBlockManager = VocabularyBlockManager.getMergedFileManager();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }


        switch (scoringFunctionType) {
            case TFIDF:
                outputFilePath = TermsUpperBoundManager.outputFileDirectory + "tfidfUpperBounds.binary";
                binaryFileManager = openUBFile(outputFilePath);
                scoreFunction = new TFIDF(collectionStatistics.getTotalDocuments());
                break;

            case BM25:
                outputFilePath = TermsUpperBoundManager.outputFileDirectory + "BM25UpperBounds.binary";
                binaryFileManager = openUBFile(outputFilePath);
                scoreFunction = new BM25(collectionStatistics.getTotalDocuments(), collectionStatistics.getAverageDocumentLength(), loadDocumentIndexLengthInformation(collectionStatistics));
                break;
        
            default:
                throw new UnsupportedOperationException("undefined scoring function");
        }
        System.out.println("Starting generation of upper bounds...");
        generate(vocabularyBlockManager, scoreFunction, binaryFileManager);
        System.out.println("Done!");

        binaryFileManager.close();
        vocabularyBlockManager.closeBlock();

    }
    /**
     * effectively generates the terms upper bounds
     */
    protected static void generate(VocabularyBlockManager vocabularyBlockManager, ScoreFunction scoreFunction, BinaryFileManager binaryFileManager){
        int index = 0;
        while (true) {

            VocabularyFileRecord vocabularyRecord;
            try {
                vocabularyRecord = vocabularyBlockManager.readRow();
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }

            if(vocabularyRecord == null){
                break;
            }

            Posting current = null;
            double termUpperBound = -1;
            // for each term, we open its iterator
            PostingListIterator iterator = PostingListIteratorFactory.openIterator(vocabularyRecord);
            System.out.print("\rProcessing term no\t" + index );
            while (iterator.hasNext()) {
                current = iterator.next();
                termUpperBound = Math.max(termUpperBound, scoreFunction.documentWeight(vocabularyRecord, current) );
            }
            //iterator.closeList();
            //here, we have iterated over the whole posting list for that term
            try {
                binaryFileManager.writeDouble(termUpperBound);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("[TermsUpperBoundGenerator] execution aborted");
                return;
            }
            index++;
        }

        System.out.println();
    }

    /**
     * function that checks if a file already exists with that file name, in that case asks to the user if the file has to be overwritten
     * then it opens the binaryFileManager for that file
     * it opens the file in write mode
     */
    private static BinaryFileManager openUBFile(String outputFilePath){
        if( ! ( new File(TermsUpperBoundManager.outputFileDirectory)).exists() ){
            try {
                Files.createDirectories(Paths.get(TermsUpperBoundManager.outputFileDirectory));
            } catch (IOException e) {
                System.out.println("[TermsUpperBoundGenerator.openUBFile]: Could not create directories for the path: " + outputFileDirectory);
                e.printStackTrace();
            }
        }
        return new BinaryFileManager(outputFilePath, MODE.WRITE);
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

        int doublesRead = -1;
        //double value;

        while(doublesRead != 0){
            try {
                doublesRead = binaryFileManager.readDoubleArray(ret);
                //value = binaryFileManager.readDouble();
                //ret.add(value);
            } catch (EOFException e) {
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ret;
    }

    /**
     * this method should be called only once at the end of the program
     * once called the iterator over the posting lists will not be accessible anymore
     */
    public static void close(){
        System.out.println();
        PostingListIteratorFactory.close();
    }

    /**
     * 
     * @return an int[] whose length is the same as the number of documents in the collection, 
     * the array element at index X contains the document length for the document of docID X
     */
    private static int[] loadDocumentIndexLengthInformation(CollectionStatistics collectionStatistics){
        DocumentIndexBlockManager documentIndexBlockManager = null;
        
        try {
            documentIndexBlockManager = DocumentIndexBlockManager.getMergedFileManager();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        int [] documentIndexLengthsInformation = new int[collectionStatistics.getTotalDocuments()];
        int i = 0;
        
        int howMany = -1;
        
        while (howMany != 0) {
            howMany = documentIndexBlockManager.readDocumentLenghtsList(documentIndexLengthsInformation, i);
            i += howMany;
        }
        
        if(i != collectionStatistics.getTotalDocuments()){
            System.out.println("[loadDocumentIndexLengthInformation] DEBUG: i: " + i + " \t numDocs: " + collectionStatistics.getTotalDocuments());
        }
        return documentIndexLengthsInformation;
    }
}
