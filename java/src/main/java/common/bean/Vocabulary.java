package common.bean;

import java.util.HashMap;

public class Vocabulary{

    private class VocabularyInformation{

        private String term; // togliere se si usa hashmap
        private int df;
        private int cf;
        private int offset;

        /*   costruttore
         metodi get per accedere a campi privati
         metodi set per settare campi privati
        * */
    }

    private HashMap<String, VocabularyInformation> vocabulary;

    /*
         metodi get e set
         sort per riordinare il vocabulary per termine
         metodo per aggiungere una voce al vocabulary
     */
}
