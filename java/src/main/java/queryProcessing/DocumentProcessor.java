package queryProcessing;

import common.bean.VocabularyFileRecord;
import queryProcessing.scoring.ScoreFunction;
import queryProcessing.QueryProcessor.*;

import java.util.List;

public abstract class DocumentProcessor {

    public static class DocumentScore  {
        private int docId;
        private double score;

        public DocumentScore(int docId, double score) {
            this.docId = docId;
            this.score = score;
        }

        public int getDocId() {
            return docId;
        }

        public double getScore() {
            return score;
        }

        @Override
        public String toString() {
            return "DocumentScore [docId=" + docId + ", score=" + score + "]";
        }

        public static int compare(DocumentScore o1, DocumentScore o2) {
            if(o1.getScore() > o2.getScore()){
                return 1;
            }else if(o1.getScore() < o2.getScore()){
                return -1;
            }else{
                if(o1.getDocId() > o2.getDocId()){
                    return -1;
                }else if(o1.getDocId() < o2.getDocId()){
                    return 1;
                }else{
                    return 0;
                }
            }
        }


    }

    public abstract List<DocumentScore> scoreDocuments(List<VocabularyFileRecord> queryTerms, ScoreFunction scoringFunction, QueryType queryType, int k);
}
