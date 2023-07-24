package common.bean;

public class VocabularyFileRecord{

    String term;
    int cf;
    int df;
    OffsetInvertedIndex offset;

    public VocabularyFileRecord(String term, int cf, int df, OffsetInvertedIndex offset) {
        this.term = term;
        this.cf = cf;
        this.df = df;
        this.offset = offset;
    }

    public String getTerm() {
        return term;
    }
    public int getCf() {
        return cf;
    }
    public int getDf() {
        return df;
    }
    public OffsetInvertedIndex getOffset() {
        return offset;
    }
}