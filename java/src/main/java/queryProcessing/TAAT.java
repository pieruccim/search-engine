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
        Map<Integer, Integer> documentQueryTermCount = new HashMap<>(); // to check whether a doc contains all query terms

        for (VocabularyFileRecord term : queryTerms) {
            PostingListIterator iterator = PostingListIteratorFactory.openIterator(term);

            while (iterator.hasNext()) {
                Posting posting = iterator.next();
                int docId = posting.getDocid();
                //add +1 for each docId I encounter throughout the posting list
                documentQueryTermCount.put(docId, documentQueryTermCount.getOrDefault(docId, 0) + 1);

                //get the score for the pair term-doc <=> the type is disjunctive
                // or conjunctive and the doc counter is equal to the query terms
                double termPartialScore = scoringFunction.documentWeight(term, posting);
                //and sum it to the accumulator
                documentScores.put(docId, documentScores.getOrDefault(docId, 0.0) + termPartialScore);
            }

            iterator.closeList();
        }

        //loop through the documentScore map
        for (Map.Entry<Integer, Double> entry : documentScores.entrySet()) {
            if(queryType == QueryType.CONJUNCTIVE){
                // if the count of query terms inside the doc is < of tot query terms we skip that doc
                if(documentQueryTermCount.get(entry.getKey()) < queryTerms.size()){
                    continue;
                }
            }
            priorityQueue.add(new DocumentScore(entry.getKey(), entry.getValue()));
            if (priorityQueue.size() > k) {
                priorityQueue.pollLast();
            }
        }

        return new ArrayList<>(priorityQueue);
    }
}