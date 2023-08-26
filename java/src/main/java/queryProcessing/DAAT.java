package queryProcessing;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import javafx.util.Pair;
import queryProcessing.manager.PostingListIterator;
import queryProcessing.manager.PostingListIteratorFactory;
import queryProcessing.scoring.ScoreFunction;
import queryProcessing.QueryProcessor.*;

import java.util.*;

public class DAAT extends DocumentProcessor {

    /**
     * @param queryTerms
     * @param queryType
     * @param k          corresponds to the best k docs to be returned
     * @return the k most fitting docs w.r.t. the submitted query
     */
    @Override
    public List<DocumentProcessor.DocumentScore> scoreDocuments(List<VocabularyFileRecord> queryTerms, ScoreFunction scoringFunction, QueryType queryType, int k) {


        TreeSet<DocumentScore> priorityQueue = new TreeSet<DocumentScore>(((Comparator<DocumentScore>)(DocumentScore::compare)).reversed());

        // create a map to store PostingListIterators for each query term
        //Map<String, PostingListIterator> iterators = new HashMap<>();
        ArrayList<Pair<VocabularyFileRecord, PostingListIterator>> termIteratorPairs = new ArrayList<Pair<VocabularyFileRecord, PostingListIterator>>();


        // initialize PostingListIterators for each query term, iterators will have inside the term
        // with the corresponding posting list to iterate over
        for (VocabularyFileRecord term : queryTerms) {
            PostingListIterator iterator = PostingListIteratorFactory.openIterator(term);
            //iterators.put(term.getTerm(), iterator);
            termIteratorPairs.add(new Pair<VocabularyFileRecord, PostingListIterator>(term, iterator));
        }

        
        if(queryType == QueryType.DISJUNCTIVE) {
            // DAAT algorithm disjunctive
            
            
            int prevMin = Integer.MAX_VALUE;
            while (true) {
                int minDocId = Integer.MAX_VALUE;
                
                boolean allListsProcessed = true;

                // find the min docID between the current PostingListIterators
                for (Pair<VocabularyFileRecord, PostingListIterator> pair : termIteratorPairs) {
                    VocabularyFileRecord vf = pair.getKey();
                    PostingListIterator iterator = pair.getValue();//iterators.get(term.getTerm());
                    if (iterator.hasNext()) {
                        allListsProcessed = false;

                        Posting currentPosting = iterator.getCurrentPosting();
                        if(currentPosting != null && currentPosting.getDocid() != prevMin){
                            // we want to process one document at a time, starting from the one with the 
                            // lowest docID, if the current posting of an iterator has a higher docID,
                            // we do not have to move forward that iterator, but we must consider that posting 
                            // for the search of the new minimum docID
                            minDocId = Math.min(minDocId, currentPosting.getDocid());
                            continue;
                        }

                        int docId = iterator.next().getDocid();//iterator.getCurrentPosting().getDocid();
                        minDocId = Math.min(minDocId, docId);
                    }
                }

                // if all Posting lists are processed, exit the loop
                if (allListsProcessed) {
                    break;
                }

                // compute the document score for the current document ID
                double score = computeDocumentScore(termIteratorPairs, minDocId, scoringFunction);

                // update the min heap with the current document score
                DocumentScore tmp = new DocumentScore(minDocId, score);
                priorityQueue.add(tmp);
                //System.out.println("Adding documentscore tuple to results: " + tmp.toString() + "current treemap size: " + priorityQueue.size());
                if (priorityQueue.size() > k) {
                    //removes from bottom
                    tmp = priorityQueue.pollLast();
                    //System.out.println("Removed the documentscore: " + tmp.toString());
                }
                prevMin = minDocId;
            }
        } else if(queryType == QueryType.CONJUNCTIVE){
            // DAAT algorithm conjunctive
            int maxDocId = 0;
            while (true){

                boolean finished = true;

                // move forward the PostingListIterators to the next posting with docId greater or equal to maxDocId

                HashMap<String, Integer> termDocIds = new HashMap<>();
                while (true) {

                    for (Pair<VocabularyFileRecord, PostingListIterator> pair : termIteratorPairs) {
                        VocabularyFileRecord term = pair.getKey();
                        PostingListIterator iterator = pair.getValue();

                        if(termDocIds.get(term.getTerm()) != null && termDocIds.get(term.getTerm()) == maxDocId){
                            continue;
                        }

                        if (iterator.hasNext()) {
                            finished = false;
                            //System.out.println("Going to call nextGEQ for iterator of term '" + term.getTerm() + "'");
                            Posting nextPosting = iterator.nextGEQ(maxDocId);
                            if(nextPosting == null){
                                // case in which the iterator reached the end
                                finished = true;
                                break;
                            }
                            int docId = nextPosting.getDocid();
                            //System.out.println("docId returned: " + docId + "\t given maxDocId: " + maxDocId);
                            termDocIds.put(term.getTerm(), docId);
                            maxDocId = Math.max(docId, maxDocId);
                        }
                        else {
                            // case in which iterator has reached the final posting,
                            // we can't find another docId that occurs in all iterators
                            finished = true;
                            break;
                        }
                    }

                    if (finished){
                        break;
                    }

                    Integer firstValue = termDocIds.isEmpty() ? null : termDocIds.values().iterator().next();
                    if (termDocIds.values().stream().allMatch(value -> value.equals(firstValue))){
                        break;
                    }
                }

                // if no more docId appear in all the iterators
                if (finished) {
                    break;
                }

                // compute the document score for the current document ID
                double score = computeDocumentScore(termIteratorPairs, maxDocId, scoringFunction);

                // update the max heap with the current document score
                DocumentScore tmp = new DocumentScore(maxDocId, score);
                priorityQueue.add(tmp);
                //System.out.println("Adding documentscore tuple to results: " + tmp.toString() + "current treemap size: " + priorityQueue.size());
                if (priorityQueue.size() > k) {
                    //removes from bottom
                    tmp = priorityQueue.pollLast();
                    //System.out.println("Removed the documentscore: " + tmp.toString());
                }

            }
        }

        // collect the k highest scoring documents from the priority queue
        List<DocumentScore> bestKdocs = new ArrayList<>();
        while (!priorityQueue.isEmpty()) {
            bestKdocs.add(priorityQueue.pollFirst());
        }

        // close all iterators
        for (Pair<VocabularyFileRecord, PostingListIterator> pair : termIteratorPairs) {
            pair.getValue().closeList();
        }

        return bestKdocs;
    }

    private double computeDocumentScore(ArrayList<Pair<VocabularyFileRecord, PostingListIterator>> termIteratorPairs, int docId, ScoreFunction scoringFunction) {

        double score = 0;

        for (Pair<VocabularyFileRecord, PostingListIterator> pair : termIteratorPairs) {
            VocabularyFileRecord term = pair.getKey();
            PostingListIterator iterator = pair.getValue();
            Posting posting = iterator.getCurrentPosting();
            if (posting != null && posting.getDocid() == docId) {
                score += scoringFunction.documentWeight(term, posting);
            }
        }
        return score;
    }
}
