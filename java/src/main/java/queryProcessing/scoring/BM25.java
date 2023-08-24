package queryProcessing.scoring;

import common.bean.*;
import common.manager.CollectionStatisticsManager;

import java.util.HashMap;

public class BM25 extends ScoreFunction {

    private double avgDocumentLength;
    private int[] documentIndexLengthInformation;
    private double k1;
    private double b;

    private static final double defaultK1 = 1.2;
    private static final double defaultB = 0.75;

    public BM25(int numDocuments, double avgDocLength, int[] documentIndexLengthInformation) {
        super(numDocuments);
        this.avgDocumentLength = avgDocLength;
        this.documentIndexLengthInformation = documentIndexLengthInformation;
        this.k1 = defaultK1;
        this.b = defaultB;
    }

    @Override
    public double documentWeight(VocabularyFileRecord term, Posting posting) {
        double idf = Math.log((numDocuments - term.getDf() + 0.5) / (term.getDf() + 0.5));
        double tf = posting.getFreq() * (k1 + 1) / (posting.getFreq() + k1 * (1 - b + b * (documentIndexLengthInformation[posting.getDocid()] / avgDocumentLength)));

        return idf * tf;
    }
}