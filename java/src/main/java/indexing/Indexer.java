package indexing;

import common.bean.DocumentIndex;
import common.bean.InvertedIndex;
import common.bean.Vocabulary;

public class Indexer {

    private Vocabulary vocabulary;
    private DocumentIndex documentIndex;
    private InvertedIndex invertedIndex;

    public Indexer(){
        vocabulary = new Vocabulary();
        documentIndex = new DocumentIndex();
        invertedIndex = new InvertedIndex();
    }

    /*
        metodi:
            -   metodo leggere un doc alla volta (skippare quelli non validi) e eseguire operazioni di preprocessing
            -   metodo che prende l'output del modulo di preprocessing (lista di token) per aggiornare vocabolario,
                creare entry del document index e aggiornare posting list del termine
     */
}
