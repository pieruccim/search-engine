package common.bean;

import java.util.ArrayList;
import java.util.HashMap;

public class InvertedIndex {

    private class PostingInformation{

        private int docid;
        private int freq;

        /*
            costruttore
            metodi get
            metodi set
         */

    }

    private ArrayList<PostingInformation> invertedIndex;

    /*      VERSIONE CHE UTILIZZA HASHMAP

    private class PostingList{

        private ArrayList<PostingInformation> PostingList;


            costruttore
            metodi get e set
            metodo per aggiungere elemento alla posting list


    }

    public HashMap<String, PostingList> invertedIndex; // <Key:termid, Value:postingList>


        metodo per aggiungere una postingList all'inverted Index */

}
