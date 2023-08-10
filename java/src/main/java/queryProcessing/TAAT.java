package queryProcessing;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import queryProcessing.manager.PostingListIterator;
import queryProcessing.manager.PostingListIteratorFactory;
import queryProcessing.scoring.ScoreFunction;
import queryProcessing.QueryProcessor.*;

import java.util.*;

public class TAAT extends DocumentProcessor {

    @Override
    public List<DocumentScore> scoreDocuments(List<VocabularyFileRecord> queryTerms, ScoreFunction scoringFunction, QueryType queryType, int k) {

        TreeSet<DocumentScore> priorityQueue = new TreeSet<DocumentScore>(((Comparator<DocumentScore>)(DocumentScore::compare)).reversed());
        Map<Integer, Double> documentScores = new HashMap<>();

        for (VocabularyFileRecord term : queryTerms) {
            PostingListIterator iterator = PostingListIteratorFactory.openIterator(term);

            while (iterator.hasNext()) {
                Posting posting = iterator.next();
                int docId = posting.getDocid();
                //get the score for the pair term-doc
                double termPartialScore = scoringFunction.documentWeight(term, posting);
                //and sum it to the accumulator
                documentScores.put(docId, documentScores.getOrDefault(docId, 0.0) + termPartialScore);
            }

            iterator.closeList();
        }

        //loop through the documentScore map
        for (Map.Entry<Integer, Double> entry : documentScores.entrySet()) {
            priorityQueue.add(new DocumentScore(entry.getKey(), entry.getValue()));
            if (priorityQueue.size() > k) {
                priorityQueue.pollLast();
            }
        }

        return new ArrayList<>(priorityQueue);
    }
}