package queryProcessing;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import queryProcessing.manager.PostingListIterator;
import queryProcessing.manager.PostingListIteratorFactory;
import queryProcessing.scoring.ScoreFunction;

import java.util.*;

public class DAAT {

    TreeSet<DocumentScore> priorityQueue = new TreeSet<>(Comparator.comparingDouble(DocumentScore::getScore).reversed());

    public static class DocumentScore {
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
    }




    /**
     *
     * @param queryTerms
     * @param k corresponds to the best k docs to be returned
     * @return the k most fitting docs w.r.t. the submitted query
     */
    public List<DocumentScore> scoreDocuments(List<VocabularyFileRecord> queryTerms, ScoreFunction scoringFunction, int k) {

        // create a map to store PostingListIterators for each query term
        Map<String, PostingListIterator> iterators = new HashMap<>();

        // initialize PostingListIterators for each query term, iterators will have inside the term
        // with the corresponding posting list to iterate over
        for (VocabularyFileRecord term : queryTerms) {
            PostingListIterator iterator = PostingListIteratorFactory.openIterator(term);
            iterators.put(term.getTerm(), iterator);
        }

        // DAAT algorithm
        while (true) {
            int minDocId = Integer.MAX_VALUE;
            boolean allListsProcessed = true;

            // find the min docID between the current PostingListIterators
            for (VocabularyFileRecord term : queryTerms) {
                PostingListIterator iterator = iterators.get(term.getTerm());
                if (iterator.hasNext()) {
                    allListsProcessed = false;
                    int docId = iterator.getCurrentPosting().getDocid();
                    minDocId = Math.min(minDocId, docId);
                }
            }

            // if all Posting lists are processed, exit the loop
            if (allListsProcessed) {
                break;
            }

            // compute the document score for the current document ID
            double score = computeDocumentScore(queryTerms, iterators, minDocId, scoringFunction);

            // update the min heap with the current document score
            priorityQueue.add(new DocumentScore(minDocId, score));
            if (priorityQueue.size() > k) {
                //removes from bottom
                priorityQueue.pollLast();
            }

            // move forward the PostingListIterators to the next document ID for terms that match the minDocId
            for (VocabularyFileRecord term : queryTerms) {
                PostingListIterator iterator = iterators.get(term.getTerm());
                iterator.nextGEQ(minDocId);
            }
        }

        // collect the k highest scoring documents from the priority queue
        List<DocumentScore> bestKdocs = new ArrayList<>();
        while (!priorityQueue.isEmpty()) {
            bestKdocs.add(priorityQueue.pollFirst());
        }

        // close all iterators
        for (PostingListIterator iterator : iterators.values()) {
            iterator.closeList();
        }

        return bestKdocs;
    }

    private double computeDocumentScore(List<VocabularyFileRecord> queryTerms, Map<String, PostingListIterator> iterators, int docId, ScoreFunction scoringFunction) {

        double score = 0;

        for (VocabularyFileRecord term : queryTerms) {
            PostingListIterator iterator = iterators.get(term.getTerm());
            Posting posting = iterator.getCurrentPosting();
            if (posting != null && posting.getDocid() == docId) {
                score += scoringFunction.documentWeight(term, posting);
            }
        }
        return score;
    }
}
