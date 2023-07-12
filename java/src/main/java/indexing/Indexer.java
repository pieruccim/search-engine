package indexing;

import common.bean.DocumentIndex;
import common.bean.InvertedIndex;
import common.bean.Vocabulary;
import javafx.util.Pair;
import preprocessing.Preprocessor;

import java.util.HashMap;
import java.util.Map;

public class Indexer {

    private Vocabulary vocabulary;
    private DocumentIndex documentIndex;
    private InvertedIndex invertedIndex;

    public Indexer(){
        this.vocabulary = new Vocabulary();
        this.documentIndex = new DocumentIndex();
        this.invertedIndex = new InvertedIndex();
    }

    private void addToIndex(String docText, int docNo){
        // TODO: Manage saving of the Block to disk when memory is almost empty (SPiMI)

        String[] terms = Preprocessor.processText(docText, false);

        // Count term frequencies in the document
        HashMap<String, Integer> termCounter = new HashMap<>();
        for (String term: terms) {
            termCounter.put(term, termCounter.containsKey(term) ? termCounter.get(term)+1 : 1);
        }

        /**System.out.println("Docu: " + docNo);

        for (Map.Entry<String, Integer> entry : termCounter.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            System.out.println("term: " + key + ", counter: " + value);
        }*/

        // TODO: Update Vocabulary, Posting List, ecc...

        invertedIndex.addPostings(docNo, termCounter);
    }

    public void processCorpus(){

        // TODO: Call to FileManager to retrieve corpus of documents: now is emulated
        String[] lines = {  "0\tThe presence of communication amid scientific minds was equally important to the success of the Manhattan Project as scientific intellect was. The only cloud hanging over the impressive achievement of the atomic researchers and engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated.\n",
                            "1\tThe Manhattan Project and its atomic bomb helped bring an end to World War II. Its legacy of peaceful uses of atomic energy continues to have an impact on history and science.\n",
                            "2\tEssay on The Manhattan Project - The Manhattan Project The Manhattan Project was to see if making an atomic bomb possible. The success of this project would forever change the world forever making it known that something this powerful can be manmade.",
                            "3\tThe Manhattan Project was the name for a project conducted during World War II, to develop the first atomic bomb. It refers specifically to the period of the project from 194 Ã¢Â€Â¦ 2-1946 under the control of the U.S. Army Corps of Engineers, under the administration of General Leslie R. Groves.",
                            "4\tversions of each volume as well as complementary websites. The first websiteÃ¢Â€Â“The Manhattan Project: An Interactive HistoryÃ¢Â€Â“is available on the Office of History and Heritage Resources website, http://www.cfo. doe.gov/me70/history. The Office of History and Heritage Resources and the National Nuclear Security",
                            "5\tThe Manhattan Project. This once classified photograph features the first atomic bomb Ã¢Â€Â” a weapon that atomic scientists had nicknamed Gadget.. The nuclear age began on July 16, 1945, when it was detonated in the New Mexico desert."};

        for (String line: lines) {

            Pair<Integer, String> lineFormatted = null;

            try {
                lineFormatted = Preprocessor.parseLine(line);
            } catch (IllegalArgumentException e){
                e.printStackTrace();
            }

            int docNo = lineFormatted.getKey();
            String docText = lineFormatted.getValue();

            addToIndex(docText, docNo);
        }

        System.out.print("manhattan: " + invertedIndex.toString("manhattan"));
    }

    /*
        metodi:
            -   metodo leggere un doc alla volta (skippare quelli non validi) e eseguire operazioni di preprocessing
            -   metodo che prende l'output del modulo di preprocessing (lista di token) per aggiornare vocabolario,
                creare entry del document index e aggiornare posting list del termine
     */
}
