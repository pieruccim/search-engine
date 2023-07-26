package queryProcessing.scoring;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class ScoreFunction {

    protected int numDocuments;

    public ScoreFunction(int numDocuments){
        this.numDocuments = numDocuments;
    }

    /**
     *
     * @param term is the current term in the query
     * @param posting is the posting list relative to the current term in the query
     * @return the document weight given a term and its posting
     */
    public abstract double documentWeight(VocabularyFileRecord term, Posting posting);
}
