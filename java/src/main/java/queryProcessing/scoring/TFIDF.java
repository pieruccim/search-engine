package queryProcessing.scoring;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;

public class TFIDF extends ScoreFunction{

    public TFIDF(int numDocuments){
        super(numDocuments);
    }

    @Override
    public double documentWeight(VocabularyFileRecord term, Posting posting) {

        double idf = Math.log(this.numDocuments/term.getDf());
        double tf = 1+Math.log(posting.getFreq());

        return tf*idf;
    }

}
