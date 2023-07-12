package common.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Vocabulary{

    private class VocabularyInformation{

        private ArrayList<Integer> df; //assume it contains the docids of the docs in which the term appears
        private int cf;

        public VocabularyInformation(ArrayList<Integer> df, int cf){
            this.df = df;
            this.cf = cf;
        }

        public ArrayList<Integer> getDf() {
            return df;
        }

        public void setDf(ArrayList<Integer> df) {
            this.df = df;
        }

        public int getCf() {
            return cf;
        }

        public void setCf(int cf) {
            this.cf = cf;
        }

        // useful for debugging
        @Override
        public String toString() {
            return "VocabularyInformation{" +
                    "df=" + this.df +
                    ", cf=" + this.cf +
                    '}';
        }

    }

    // the vocabulary is implemented by an HashMap that links a term (string) and the corresponding information (df,cf,offset)
    private HashMap<String, VocabularyInformation> vocabulary;

    public Vocabulary(){
        this.vocabulary = new HashMap<String, VocabularyInformation>();
    }

    public HashMap<String, VocabularyInformation> getVocabulary() {
        return vocabulary;
    }

    public void setVocabulary(HashMap<String, VocabularyInformation> v) {
        this.vocabulary = v;
    }

    // Method to return a sorted list of terms from the vocabulary
    public List<String> getSortedTerms() {
        List<String> sortedTerms = new ArrayList<>(vocabulary.keySet());
        sortedTerms.sort(String::compareTo);
        return sortedTerms;
    }

    // NB: assuming df contains the list of docids where a certain term appears
    public void addInformation(String term, int docid) {
        VocabularyInformation information = vocabulary.get(term);

        if (information != null) {
            // Term already exists in the vocabulary
            ArrayList<Integer> dfList = information.getDf();
            if (!dfList.contains(docid)) {
                // docid is not present in the df list, add it
                dfList.add(docid);
                information.setCf(information.getCf() + 1);
            }
            else{
                // docid is present in the df list so just increment total occurrence (cf)
                information.setCf(information.getCf() + 1);
            }
        } else {
            // Term does not exist, create a new entry in the hashmap
            ArrayList<Integer> newDfList = new ArrayList<>();
            newDfList.add(docid);
            VocabularyInformation newInformation = new VocabularyInformation(newDfList, 1);
            vocabulary.put(term, newInformation);
        }
    }


    // print df, cf and offset given a term
    public String toString(String term) {
        VocabularyInformation information = vocabulary.get(term);
        return (information != null) ? "Term: " + term + ", " + information.toString() : "Term not found in vocabulary.";
    }

}
