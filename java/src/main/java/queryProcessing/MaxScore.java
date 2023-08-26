package queryProcessing;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import common.bean.VocabularyFileRecordUB;
import queryProcessing.QueryProcessor.QueryType;
import queryProcessing.manager.PostingListIterator;
import queryProcessing.manager.PostingListIteratorFactory;
import queryProcessing.scoring.ScoreFunction;

public class MaxScore extends DocumentProcessor{

    protected double threshold;

    public MaxScore(double threshold){
        this.threshold = threshold;
    }

    /**
     * this method must receive a List of VocabularyFileRecordUB, so that it can access to the upperbound information for each query term
     */
    @Override
    public List<DocumentScore> scoreDocuments(List<VocabularyFileRecord> queryTerms, ScoreFunction scoringFunction,
            QueryType queryType, int k) {

            throw new UnsupportedOperationException();

        }

    protected static TreeSet<DocumentScore> priorityQueue = new TreeSet<DocumentScore>(((Comparator<DocumentScore>)(DocumentScore::compare)).reversed());
    protected static final Comparator<? super VocabularyFileRecordUB> comparatorUB = Comparator.comparing(VocabularyFileRecordUB::getUpperBound);
    
    List<VocabularyFileRecordUB> qTNonEssential = new ArrayList<VocabularyFileRecordUB>();
    private static List<VocabularyFileRecordUB> qTEssential = new ArrayList<VocabularyFileRecordUB>();
    // we will also load the posting lists iterators
    private static List<PostingListIterator> allIterators = new ArrayList<PostingListIterator>();
    private static List<PostingListIterator> essentialIterators    = new ArrayList<PostingListIterator>();
    private static List<PostingListIterator> nonEssentialIterators = new ArrayList<PostingListIterator>();
    /**
     * this method must receive a List of VocabularyFileRecordUB, so that it can access to the upperbound information for each query term
     */
    public List<DocumentScore> scoreDocumentsUB(List<VocabularyFileRecordUB> qT, ScoreFunction scoringFunction,
            QueryType queryType, int k) {

        priorityQueue.clear();
        
        // here we have to split the posting lists between the essential and the non-essential ones

        qT.sort(comparatorUB);

        qTEssential.clear();
        qTNonEssential.clear();

        allIterators.clear();
        essentialIterators.clear();
        nonEssentialIterators.clear();

        double sum = 0;
        double nonEssentialTermUpperBound = 0;

        for (int i = 0; i < qT.size(); i++) {
            sum += qT.get(i).getUpperBound();

            if(sum < threshold){
                qTNonEssential.add(qT.get(i));
                nonEssentialIterators.add(PostingListIteratorFactory.openIterator(qT.get(i)));
                nonEssentialTermUpperBound = sum;//+= qT.get(i).getUpperBound();
            }else{
                qTEssential.add(qT.get(i));
                essentialIterators.add(PostingListIteratorFactory.openIterator(qT.get(i)));
            }
        }

        // the objects will not be copied; references to the same objects will be added to the list
        allIterators.addAll(essentialIterators);
        allIterators.addAll(nonEssentialIterators);

        // here we perform DAAT over the essentialIterators

        // we need to obtain the lowest docID's score among the docIDs that are present in the essential posting lists
        int currentDocId = -1;
        double minScoreThreshold = -1;

        double currentPartialScore;

        boolean essentialPostingListAreEnded = false;
        while( ! essentialPostingListAreEnded ){

            if(queryType == QueryType.DISJUNCTIVE){
                currentDocId = getNextDocumentId(essentialIterators, currentDocId);
            }else{
                // when query type is conjunctive, the next docID must be retrieved by checking 
                // all the posting lists iterators
                currentDocId = getNextDocumentIdConjunctive(allIterators, currentDocId);

            }
            if(currentDocId == -1){
                essentialPostingListAreEnded = true;
                continue;
            }

            currentPartialScore = computeDocumentScore(qTEssential, essentialIterators, currentDocId, scoringFunction);
            //System.out.println("docID: " + currentDocId + "\t partialScore: " + currentPartialScore);
            double currentUpperBound = currentPartialScore + nonEssentialTermUpperBound;

            if(currentUpperBound < threshold){
                // if the term upper bound is lower than the thresold,
                // we can skip that document
                continue;
            }


            // sum the term upper bounds for all non-essential posting lists
            for (int i = nonEssentialIterators.size() - 1; i >= 0; i--) {
                // we start computing the effective score for the document in each posting list starting from the one with
                // the highest upperbound


                PostingListIterator currentIterator = nonEssentialIterators.get(i);
                VocabularyFileRecordUB currentVocabularyFileRecord = qTNonEssential.get(i);

                Posting posting = currentIterator.getCurrentPosting();

                if (posting != null && posting.getDocid() == currentDocId) {
                    currentUpperBound += scoringFunction.documentWeight(currentVocabularyFileRecord, posting);
                }else if(currentIterator.hasNext() && queryType != QueryType.CONJUNCTIVE){ 
                    // when the query type is conjunctive, the iterators are moved only by the method getNextDocumentIdConjunctive
                    posting = currentIterator.nextGEQ(currentDocId);
                    if(posting != null && posting.getDocid() == currentDocId){
                    currentUpperBound += scoringFunction.documentWeight(currentVocabularyFileRecord, posting);
                    }
                }
                
                // we can remove the upper bound relative to the current posting list, since we computed the document's effective score for that posting list
                currentUpperBound -= currentVocabularyFileRecord.getUpperBound();

                if(currentUpperBound < threshold){
                    // if the term upper bound is lower than the thresold,
                    // we can skip that document
                    break;
                }
            }
            if(currentUpperBound < threshold){
                // skip the document
                continue;
            }

            // if we arrive here, we have the effective document score inside currentUpperBound and the docId inside currentDocId
            // uadd the current document score
            if(currentUpperBound > minScoreThreshold){
                DocumentScore tmp = new DocumentScore(currentDocId, currentUpperBound);
                priorityQueue.add(tmp);
                //System.out.println("Adding documentscore tuple to results: " + tmp.toString() + "current treemap size: " + priorityQueue.size());
                if (priorityQueue.size() > k) {
                    //removes from bottom
                    tmp = priorityQueue.pollLast();
                    minScoreThreshold = tmp.getScore();
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
        for (PostingListIterator iterator : allIterators) {
            iterator.closeList();
        }

        return bestKdocs;
    }

    /**
     * returns the next docid that the DAAT algorithm finds in the given list of postinglists
     * - advances the given iterators to the selected nextDocId
     * - returns -1 in case there are no other documents to be processed
     */
    private int getNextDocumentId(List<PostingListIterator> postingListIterators,
         int lastDocId){

            int nextDocId = Integer.MAX_VALUE;
            Posting posting = null;
            boolean allDone = true;

            for (PostingListIterator postingListIterator : postingListIterators) {

                posting = postingListIterator.getCurrentPosting();

                if(posting != null && posting.getDocid() > lastDocId){
                    nextDocId = Math.min(nextDocId, posting.getDocid());
                    allDone = false;
                    continue;
                }

                if(postingListIterator.hasNext()){
                    posting = postingListIterator.next();
                    nextDocId = Math.min(nextDocId, posting.getDocid());
                    allDone = false;
                }
            }

            if(allDone == true){
                // case in which no new document id was found, it means that the DAAT is completed
                return -1;
            }

            return nextDocId;
    }

    /**
     * returns the next docid that the CONJUNCTIVE DAAT algorithm finds in the given list of postinglists
     * - advances the given iterators to the selected nextDocId
     * - returns -1 in case there are no other documents to be processed
     */
    private int getNextDocumentIdConjunctive(List<PostingListIterator> allIterators,
         int lastDocId){

            int nextDocId = -1;
            Posting posting = null;
            boolean allDone = true;

            boolean converged = false;

            while( ! converged){

                converged = true;

                for (PostingListIterator postingListIterator : allIterators) {

                    posting = postingListIterator.getCurrentPosting();

                    if(posting != null && posting.getDocid() > lastDocId){

                        allDone = false;

                        if(nextDocId != posting.getDocid()){
                            nextDocId = Math.max(nextDocId, posting.getDocid());
                            converged = false;
                            // the current for loop iteration must continue so that the method nextGEQ is called
                        }else{       
                            // when the posting list is already at the position of the current nextDocId                  
                            continue;
                        }
                    }

                    if(postingListIterator.hasNext()){
                        posting = postingListIterator.nextGEQ(nextDocId);
                        if(posting == null){
                            // case in which one of the posting lists is finished:
                            // no documentId can be found in all the iterators if one of them is finished
                            return -1;
                        }
                        
                        allDone = false;

                        if(nextDocId != posting.getDocid()){
                            nextDocId = Math.max(nextDocId, posting.getDocid());
                            converged = false;
                        }
                    }else{
                        // case in which an iterator is finished
                        return -1;
                    }
                }

            }

            if(allDone == true){
                // case in which no new document id was found, it means that the DAAT is completed
                return -1;
            }

            return nextDocId;
    }

    private double computeDocumentScore(List<VocabularyFileRecordUB> queryTerms, List<PostingListIterator> iterators, int docId, ScoreFunction scoringFunction) {

        double score = 0;

        if(queryTerms.size() != iterators.size()){
            throw new UnsupportedOperationException("the lists of vocabularyRecords and of Posting List iterators must be of the same size");
        }

        for (int i = 0; i < queryTerms.size(); i++) {
            PostingListIterator iterator = iterators.get(i);
            Posting posting = iterator.getCurrentPosting();
            if (posting != null && posting.getDocid() == docId) {
                score += scoringFunction.documentWeight(queryTerms.get(i), posting);
            }
        }
        return score;
    }
    
}
