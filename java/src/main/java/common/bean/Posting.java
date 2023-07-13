package common.bean;

import java.util.ArrayList;
import java.util.HashMap;

public class Posting {

    private int docid;
    private int freq;

    public Posting(int docid, int freq){
        this.docid = docid;
        this.freq = freq;
    }

    public int getDocid() {
        return docid;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public int getFreq() {
        return freq;
    }

    public void setFreq(int freq) {
        this.freq = freq;
    }

    @Override
    public String toString() {
        return " -> {" +
                "docid=" + this.docid +
                ", freq=" + this.freq +
                "}";
    }
}

