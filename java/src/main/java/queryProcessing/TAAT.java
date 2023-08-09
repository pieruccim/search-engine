package queryProcessing;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import queryProcessing.manager.PostingListIterator;
import queryProcessing.manager.PostingListIteratorFactory;
import queryProcessing.scoring.ScoreFunction;

import java.util.*;

public class TAAT extends DocumentProcessor {

    @Override
    public List<DocumentScore> scoreDocuments(List<VocabularyFileRecord> queryTerms, ScoreFunction scoringFunction, QueryProcessor.QueryType queryType, int k) {

        TreeSet<DocumentScore> priorityQueue = new TreeSet<DocumentScore>(((Comparator<DocumentScore>)(DocumentScore::compare)).reversed());
        Map<String, PostingListIterator> iterators = new HashMap<>();

        for (VocabularyFileRecord term : queryTerms) {
            PostingListIterator iterator = PostingListIteratorFactory.openIterator(term);
            iterators.put(term.getTerm(), iterator);
        }

        // Compute the document scores for each term and accumulate
        while (true) {
            boolean allListsProcessed = true;

            double totalScore = 0;

            for (VocabularyFileRecord term : queryTerms) {
                PostingListIterator iterator = iterators.get(term.getTerm());

                if (iterator.hasNext()) {
                    allListsProcessed = false;

                    Posting posting = iterator.next();
                    totalScore += scoringFunction.documentWeight(term, posting);
                }
            }

            if (allListsProcessed) {
                break;
            }

            // obtain the current document ID
            int docId = iterators.values().iterator().next().getCurrentPosting().getDocid();
            priorityQueue.add(new DocumentScore(docId, totalScore));

            if (priorityQueue.size() > k) {
                priorityQueue.pollLast();
            }
        }

        List<DocumentScore> bestKdocs = new ArrayList<>(priorityQueue);

        for (PostingListIterator iterator : iterators.values()) {
            iterator.closeList();
        }

        return bestKdocs;
    }

}
