package queryProcessing;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import queryProcessing.manager.PostingListIterator;
import queryProcessing.manager.PostingListIteratorFactory;
import queryProcessing.scoring.ScoreFunction;
import queryProcessing.QueryProcessor.*;

import java.util.*;

public class TAAT extends DocumentProcessor {

    private static TreeSet<DocumentScore> priorityQueue = new TreeSet<DocumentScore>(((Comparator<DocumentScore>)(DocumentScore::compare)).reversed());
    private static Map<Integer, Double> documentScores = new HashMap<>();
    private static ArrayList<Integer> docIds = new ArrayList<Integer>();
    private static ArrayList<Integer> docIdsNext = new ArrayList<Integer>();
    private static ArrayList<Integer> swap;

    @Override
    public List<DocumentScore> scoreDocuments(List<VocabularyFileRecord> queryTerms, ScoreFunction scoringFunction, QueryType queryType, int k) {

        priorityQueue.clear();
        documentScores.clear();

        if(queryType == QueryType.CONJUNCTIVE){
            this.docIds.clear();
            this.docIdsNext.clear();
        }
        //Map<Integer, Integer> documentQueryTermCount = new HashMap<>(); // to check whether a doc contains all query terms

        boolean isFirstIterator = true;

        for (VocabularyFileRecord term : queryTerms) {
            PostingListIterator iterator = PostingListIteratorFactory.openIterator(term);


            while (iterator.hasNext()) {
                Posting posting = iterator.next();
                int docId = posting.getDocid();
                //add +1 for each docId I encounter throughout the posting list
                //documentQueryTermCount.put(docId, documentQueryTermCount.getOrDefault(docId, 0) + 1);

                
                //and sum it to the accumulator
                if(isFirstIterator){
                    //get the score for the pair term-doc <=> the type is disjunctive
                    // or conjunctive and the doc counter is equal to the query terms
                    documentScores.put(docId, scoringFunction.documentWeight(term, posting));

                    if(queryType == QueryType.CONJUNCTIVE){
                        docIds.add(docId);
                    }
                }else{
                    Double prevScore = documentScores.get(docId);
                    if(queryType == QueryType.DISJUNCTIVE || docIds.contains(docId)){
                        documentScores.put(docId, ((prevScore != null) ? prevScore: 0.0 ) + scoringFunction.documentWeight(term, posting) );
                        if(queryType == QueryType.CONJUNCTIVE){
                            docIdsNext.add(docId);
                        }
                    }
                }

            }
            if( ! isFirstIterator){
                swap = docIds;
                docIds = docIdsNext;
                docIdsNext = swap;
                docIdsNext.clear();
            }
            isFirstIterator = false;

            //iterator.close();
        }

        //loop through the documentScore map
        double minScore = -1;

        for (Map.Entry<Integer, Double> entry : documentScores.entrySet()) {
            //if(queryType == QueryType.CONJUNCTIVE){
            //    // if the count of query terms inside the doc is < of tot query terms we skip that doc
            //    if(documentQueryTermCount.get(entry.getKey()) < queryTerms.size()){
            //        continue;
            //    }
            //}
            if(entry.getValue() > minScore){
                priorityQueue.add(new DocumentScore(entry.getKey(), entry.getValue()));
                if (priorityQueue.size() > k) {
                    minScore = priorityQueue.pollLast().getScore();
                }
            }
        }
        return new ArrayList<>(priorityQueue);
    }
}