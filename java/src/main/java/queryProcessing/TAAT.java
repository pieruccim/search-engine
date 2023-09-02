package queryProcessing;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import javafx.util.Pair;
import queryProcessing.manager.PostingListIterator;
import queryProcessing.manager.PostingListIteratorFactory;
import queryProcessing.scoring.ScoreFunction;
import queryProcessing.QueryProcessor.*;

import java.util.*;

public class TAAT extends DocumentProcessor {

    private static TreeSet<DocumentScore> priorityQueue = new TreeSet<DocumentScore>(((Comparator<DocumentScore>)(DocumentScore::compare)).reversed());
    private static Map<Integer, Double> documentScores = new HashMap<>();
    private static Set<Integer> docIds = new HashSet<Integer>();
    private static Set<Integer> docIdsNext = new HashSet<Integer>();
    private static Set<Integer> swap;
    private static final Comparator<? super VocabularyFileRecord> comparator = Comparator.comparing(VocabularyFileRecord::getDf);

    @Override
    public List<DocumentScore> scoreDocuments(List<VocabularyFileRecord> queryTerms, ScoreFunction scoringFunction, QueryType queryType, int k) {

        if(queryType == QueryType.CONJUNCTIVE){
            return scoreDocumentsCONJUNCTIVE(queryTerms, scoringFunction, k);
        }
        priorityQueue.clear();
        documentScores.clear();

        boolean isFirstIterator = true;

        for (VocabularyFileRecord term : queryTerms) {
            PostingListIterator iterator = PostingListIteratorFactory.openIterator(term);


            while (iterator.hasNext()) {
                Posting posting = iterator.next();
                int docId = posting.getDocid();
                //add +1 for each docId I encounter throughout the posting list                
                //and sum it to the accumulator
                if(isFirstIterator){
                    //get the score for the pair term-doc <=> the type is disjunctive
                    // or conjunctive and the doc counter is equal to the query terms
                    documentScores.put(docId, scoringFunction.documentWeight(term, posting));

                }else{
                        Double prevScore = documentScores.get(docId);
                        documentScores.put(docId, ((prevScore != null) ? prevScore: 0.0 ) + scoringFunction.documentWeight(term, posting) );
                }

            }
            if( ! isFirstIterator ){
                if(docIds.size() != docIdsNext.size()){
                    swap = docIds;
                    docIds = docIdsNext;
                    docIdsNext = swap;
                }
                docIdsNext.clear();
            }
            isFirstIterator = false;

            //iterator.close();
        }

        //loop through the documentScore map
        double minScore = -1;

        for (Map.Entry<Integer, Double> entry : documentScores.entrySet()) {

            if(entry.getValue() > minScore){
                priorityQueue.add(new DocumentScore(entry.getKey(), entry.getValue()));
                if (priorityQueue.size() > k) {
                    minScore = priorityQueue.pollLast().getScore();
                }
            }
        }
        return new ArrayList<>(priorityQueue);
    }

    ArrayList<Pair<VocabularyFileRecord,PostingListIterator>> pairList = new ArrayList<Pair<VocabularyFileRecord,PostingListIterator>>();

    ArrayList<Integer> _conjDocIds = new ArrayList<Integer>();


    private List<DocumentScore> scoreDocumentsCONJUNCTIVE(List<VocabularyFileRecord> queryTerms, ScoreFunction scoringFunction, int k) {
        
        pairList.clear();
        _conjDocIds.clear();
        documentScores.clear();
        priorityQueue.clear();

        queryTerms.sort(comparator);

        PostingListIteratorFactory.openIterators(queryTerms, pairList);

        boolean isFirstIterator = true;

        Posting currentPosting = null;

        for (Pair<VocabularyFileRecord,PostingListIterator> pair : pairList) {

            VocabularyFileRecord termInfos = pair.getKey();
            PostingListIterator iterator = pair.getValue();


            if(isFirstIterator){

                while(iterator.hasNext()){
                    currentPosting = iterator.next();
                    _conjDocIds.add(currentPosting.getDocid());
                    documentScores.put(currentPosting.getDocid(), scoringFunction.documentWeight(termInfos, currentPosting));
                }

                isFirstIterator = false;
            }
            else{

                int index = 0;

                while(index < _conjDocIds.size()){ 

                    int currentDocId = _conjDocIds.get(index);

                    currentPosting = iterator.nextGEQ(currentDocId);

                    if(currentPosting == null){
                        //case in which the posting iterator is ended
                        // I have to discard all the docids in docids iterator that comes next
                        
                        _conjDocIds.subList(index, _conjDocIds.size()).clear();

                    }else if(currentPosting.getDocid() > currentDocId){
                        
                        int start = index;
                        int end = index;

                        while( end < _conjDocIds.size() && (currentDocId = _conjDocIds.get(end)) < currentPosting.getDocid()){
                            
                            end++;

                        }

                        _conjDocIds.subList(start, end).clear();

                    }else if(currentPosting.getDocid() == currentDocId){
                        Double prevScore = documentScores.get(currentDocId);
                        documentScores.put(currentDocId, prevScore + scoringFunction.documentWeight(termInfos, currentPosting) );
                        
                        index++;
                    }

                }
            }
            
        }
        
        double minScore = -1;

        for(Integer dId : _conjDocIds){

            Double score = documentScores.get(dId);

            if(score > minScore){
                priorityQueue.add(new DocumentScore(dId, score));
                if (priorityQueue.size() > k) {
                    minScore = priorityQueue.pollLast().getScore();
                }
            }
        }
        return new ArrayList<>(priorityQueue);
    }
}