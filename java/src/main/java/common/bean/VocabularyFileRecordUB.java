package common.bean;

public class VocabularyFileRecordUB extends VocabularyFileRecord{
    
    protected double upperBound;

    public VocabularyFileRecordUB(VocabularyFileRecord vocabularyFileRecord, double upperBound){
        super(vocabularyFileRecord.term, vocabularyFileRecord.cf, vocabularyFileRecord.df, vocabularyFileRecord.offset, vocabularyFileRecord.howManyBlocks);
        this.upperBound = upperBound;
    }

    public double getUpperBound() {
        return upperBound;
    }
}
