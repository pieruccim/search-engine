package common.bean;

public class VocabularyFileRecord{

    String term;
    int cf;
    int df;
    int offset;

    public VocabularyFileRecord(String term, int cf, int df, int offset) {
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
    public int getOffset() {
        return offset;
    }
}