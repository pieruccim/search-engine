package common.bean;

import java.util.ArrayList;
import java.util.HashMap;

public class InvertedIndex {

    private HashMap<String, ArrayList<PostingInformation>> invertedIndex; // <Key:termid, Value:PostingInformation>

    private class PostingInformation{

        private int docid;
        private int freq;

        public PostingInformation(int docid, int freq){
            this.docid = docid;
            this.freq = freq;
        }

        public int getDocid() {
            return docid;
        }

        public void setDocid(int docid) {
            this.docid = docid;
        }

        public int getFreq() {
            return freq;
        }

        public void setFreq(int freq) {
            this.freq = freq;
        }

        @Override
        public String toString() {
            return "{" +
                    "docid=" + this.docid +
                    ", freq=" + this.freq +
                    "} -> ";
        }
    }

    public InvertedIndex(){
        this.invertedIndex = new HashMap<String, ArrayList<PostingInformation>>();
    }

    public HashMap<String, ArrayList<PostingInformation>> getInvertedIndex() {
        return invertedIndex;
    }
    
    public void setInvertedIndex(HashMap<String, ArrayList<PostingInformation>> invertedIndex) {
        this.invertedIndex = invertedIndex;
    }

    public void addPostings(int docid, HashMap<String, Integer> termCounter){
        for (HashMap.Entry<String, Integer> entry : termCounter.entrySet()) {
            String term = entry.getKey();
            Integer frequency = entry.getValue();
            ArrayList<PostingInformation> currentPostingList = invertedIndex.get(term);
            // case where posting list doesn't exist
            if(currentPostingList == null){
                ArrayList<PostingInformation> newPostingList = new ArrayList<>();
                newPostingList.add(new PostingInformation(docid, frequency));
                invertedIndex.put(term, newPostingList);
            }
            // case where posting list already exists
            else{
                invertedIndex.get(term).add(new PostingInformation(docid, frequency));
            }
        }
    }

    // useful for debugging
    public String toString(String term) {
        ArrayList<PostingInformation> informationArray = invertedIndex.get(term);
        String result="";
        for (PostingInformation information:informationArray) {
            result += information.toString();
        }
        return result;
    }

}
