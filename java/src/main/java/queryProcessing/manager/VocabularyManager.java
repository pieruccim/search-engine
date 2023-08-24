package queryProcessing.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import common.bean.VocabularyFileRecord;
import common.bean.VocabularyFileRecordUB;
import common.manager.block.VocabularyBlockManager;
import queryProcessing.QueryProcessor.ScoringFunction;
import queryProcessing.pruning.TermsUpperBoundManager;

public class VocabularyManager {

    public static HashMap<String, VocabularyFileRecord> loadVocabulary(){

        HashMap<String, VocabularyFileRecord> vocabulary = new HashMap<String, VocabularyFileRecord>();

        VocabularyBlockManager vocabularyBlockManager = null;
        
        try {
            vocabularyBlockManager = VocabularyBlockManager.getMergedFileManager();
        } catch (IOException e) {
            e.printStackTrace();
        }
        VocabularyFileRecord vocabularyFileRecord = null;

        try {
            while(( vocabularyFileRecord = vocabularyBlockManager.readRow()) != null){
                vocabulary.put(vocabularyFileRecord.getTerm(), vocabularyFileRecord);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return vocabulary;
    }

    public static HashMap<String, VocabularyFileRecordUB> loadVocabularyWithUBs(ScoringFunction scoringFunctionType){

        HashMap<String, VocabularyFileRecordUB> retVocabulary = new HashMap<String, VocabularyFileRecordUB>();
        
        HashMap<String, VocabularyFileRecord> vocabulary = loadVocabulary();

        ArrayList<Double> upperBounds = TermsUpperBoundManager.loadUpperBoundsFromFile(scoringFunctionType);

        if(vocabulary.size() != upperBounds.size()){
            throw new UnsupportedOperationException("vocabulary.size() != upperBounds.size()\t : " + vocabulary.size() + " != " + upperBounds.size());
        }

        int index = 0;
        for (VocabularyFileRecord vocabularyFileRecord : vocabulary.values()) {
            retVocabulary.put(vocabularyFileRecord.getTerm(), new VocabularyFileRecordUB(vocabularyFileRecord, upperBounds.get(index)));
            index++;
        }

        return retVocabulary;
    }
    
}
