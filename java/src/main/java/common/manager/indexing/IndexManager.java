package common.manager.indexing;

import common.bean.Posting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class IndexManager {

    private HashMap<String, IndexRecord> index;

    public class IndexRecord{
        private int cf = 0;
        private int df = 0;
        private ArrayList<Posting> postingList = new ArrayList<>();

        public IndexRecord(int docId, int frequency){
            addPosting(docId, frequency);
        }

        public void addPosting(int docId, int frequency){
            this.cf += frequency;
            this.df += 1;
            this.postingList.add(new Posting(docId, frequency));
        }

        public ArrayList<Posting> getPostingList(){
            return this.postingList;
        }

        public int getCf() {
            return this.cf;
        }

        public int getDf() {
            return this.df;
        }
    }

    public IndexManager(){
        this.index = new HashMap<String, IndexRecord>();
    }

    public void reset(){
        this.index = new HashMap<String, IndexRecord>();
    }

    public void addPostings(int docId, HashMap<String, Integer> termCounter){
        for (HashMap.Entry<String, Integer> entry : termCounter.entrySet()) {
            String term = entry.getKey();
            Integer frequency = entry.getValue();
            IndexRecord record = index.get(term);

            // case where Record doesn't exist
            if (record == null){
                record = new IndexRecord(docId, frequency);
                index.put(term, record);
            }
            // case where Record already exists
            else {
                index.get(term).addPosting(docId, frequency);
            }
        }
    }

    /**
     * 
     * @param term
     * @return Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     */
    public IndexRecord getRecord(String term){
        return this.index.get(term);
    }

    // Debug method to display entire posting list
    public String toString(){

        String result="";

        for (HashMap.Entry<String, IndexRecord> entry : index.entrySet()) {
            String term = entry.getKey();
            ArrayList<Posting> postingList = entry.getValue().postingList;
            result += term;

            for (Posting information:postingList) {
                result += information.toString();
            }
            result += "\n";
        }
        return result;
    }

    // Debug method to display posting list of a single term
    public String toString(String term) {

        IndexRecord record = index.get(term);

        if (record == null){
            return "Term is not present in inverted index";
        }
        else{
            ArrayList<Posting> informationArray = index.get(term).postingList;
            String result = term;

            for (Posting information:informationArray) {
                result += information.toString();
            }
            return result;
        }
    }

    /**
     * @return ArrayList<String> the list of the terms that are present in the index, sorted lexicographically
     */
    public ArrayList<String> getSortedKeys(){
        ArrayList<String> terms = new ArrayList<>(this.index.keySet());
        Collections.sort(terms);
        return terms;
    }


}
