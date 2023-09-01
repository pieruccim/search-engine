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

        //if(queryType == QueryType.CONJUNCTIVE){
        //    docIds.clear();
        //    docIdsNext.clear();
        //}
        //Map<Integer, Integer> documentQueryTermCount = new HashMap<>(); // to check whether a doc contains all query terms

        boolean isFirstIterator = true;
        //if(queryType == QueryType.CONJUNCTIVE){
        //    //sort by increasing number of documents, so that in CONJUNCTIVE mode less documents will be processed
        //    queryTerms.sort(comparator);
        //}

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

                    //if(queryType == QueryType.CONJUNCTIVE){
                    //    docIds.add(docId);
                    //}
                }else{
                    //if(queryType == QueryType.DISJUNCTIVE || docIds.contains(docId)){
                        Double prevScore = documentScores.get(docId);
                        documentScores.put(docId, ((prevScore != null) ? prevScore: 0.0 ) + scoringFunction.documentWeight(term, posting) );
                        //if( ! isFirstIterator && queryType == QueryType.CONJUNCTIVE){
                        //    docIdsNext.add(docId);
                        //}
                    //}
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
            //if(queryType == QueryType.CONJUNCTIVE){
            //    if( ! docIds.contains(entry.getKey())){
            //        continue;
            //    }
            //}
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
                //Iterator<Integer> itDocIds = _conjDocIds.iterator();
                int index = 0;

                while(index < _conjDocIds.size()){    //itDocIds.hasNext()

                    int currentDocId = _conjDocIds.get(index);//itDocIds.next();

                    currentPosting = iterator.nextGEQ(currentDocId);

                    if(currentPosting == null){
                        //case in which the posting iterator is ended
                        // I have to discard all the docids in docids iterator that comes next
                        
                        _conjDocIds.subList(index, _conjDocIds.size()).clear();
                        /*itDocIds.remove();
                        while(itDocIds.hasNext()){
                            currentDocId = itDocIds.next();
                            itDocIds.remove();
                        }*/

                    }else if(currentPosting.getDocid() > currentDocId){
                        
                        int start = index;
                        int end = index;

                        while( end < _conjDocIds.size() && (currentDocId = _conjDocIds.get(end)) < currentPosting.getDocid()){
                            
                            end++;

                        }

                        _conjDocIds.subList(start, end).clear();
                        
                        /*while(currentDocId < currentPosting.getDocid()){
                            itDocIds.remove();
                            if(itDocIds.hasNext()){
                                currentDocId = itDocIds.next();
                            }else{
                                break;
                            }
                        }*/

                    }else if(currentPosting.getDocid() == currentDocId){
                        Double prevScore = documentScores.get(currentDocId);
                        documentScores.put(currentDocId, prevScore + scoringFunction.documentWeight(termInfos, currentPosting) );
                        
                        index++;
                    }

                }
            }
            
        }
        
        double minScore = -1;

        //for (Map.Entry<Integer, Double> entry : documentScores.entrySet()) {
//
        //    if( ! _conjDocIds.contains(entry.getKey())){
        //        continue;
        //    }
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