package common.bean;

public class VocabularyFileRecord{

    String term;
    int cf;
    int df;
    int offset;
    int howManyBlocks;

    public VocabularyFileRecord(String term, int cf, int df, int offset, int howManyBlocks) {
        this.term = term;
        this.cf = cf;
        this.df = df;
        this.offset = offset;
        this.howManyBlocks = howManyBlocks;
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
    public int getHowManySkipBlocks(){
        return howManyBlocks;
    }

    @Override
    public String toString() {
        return "VocabularyFileRecord [term=" + term + ", cf=" + cf + ", df=" + df + ", offset=" + offset + ", howManyBlocks="+howManyBlocks+"]";
    }
}