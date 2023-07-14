package indexing;

import common.bean.DocumentIndex;
import common.bean.DocumentIndexFileRecord;
import common.bean.Posting;
import common.bean.VocabularyFileRecord;
import common.manager.block.DocumentIndexBlockManager;
import common.manager.block.InvertedIndexBlockManager;
import common.manager.block.VocabularyBlockManager;
import common.manager.file.TextualFileManager;
import common.manager.file.FileManager.MODE;
import common.manager.indexing.IndexManager;
import common.manager.indexing.IndexManager.IndexRecord;
import javafx.util.Pair;
import preprocessing.Preprocessor;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.HashMap;

public class Indexer {

    static final private int memoryOccupationThreshold = 50;
    static private int docIdCounter = 0;
    static private int currentBlockNo = 0;

    private DocumentIndex documentIndex;
    private IndexManager indexManager;

    public Indexer(){
        this.documentIndex = new DocumentIndex();
        this.indexManager = new IndexManager();
    }

    private static float getMemoryUsage(boolean debug){
        float usedMemoryBytes = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        float totalMemoryBytes = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted();
        float memoryUsagePercentage = (usedMemoryBytes / totalMemoryBytes) * 100;

        if(debug) {
            System.out.println("Memory occupation percentage: " + Math.round(memoryUsagePercentage * 100.0f) / 100.0f + "%");
        }
        return memoryUsagePercentage;
    }


    private void resetDataStructures(){
        documentIndex.reset();
        indexManager.reset();
        // Force garbage collector to free memory
        System.gc();
    }

    private void processDocument(String docText, int docId, int docNo){

        // Manage saving of the Block to disk when memory is above threshold (SPiMI Algorithm)
        if (getMemoryUsage(true) >= memoryOccupationThreshold){
            saveBlock();
            resetDataStructures();
        };

        String[] terms = Preprocessor.processText(docText, false);

        // Count term frequencies in the document
        HashMap<String, Integer> termCounter = new HashMap<>();
        for (String term: terms) {
            termCounter.put(term, termCounter.containsKey(term) ? termCounter.get(term)+1 : 1);
        }

        // Update Document Index, Vocabulary and InvertedIndex

        documentIndex.addInformation(docText.length(), docId, docNo);
        indexManager.addPostings(docId, termCounter);
    }

    public void processCorpus(){

        // TODO: Call to FileManager to retrieve corpus of documents: now is emulated
        //String[] lines = {  "0\tThe presence of communication amid scientific minds was equally important to the success of the Manhattan Project as scientific intellect was. The only cloud hanging over the impressive achievement of the atomic researchers and engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated.\n",
        //                    "1\tThe Manhattan Project and its atomic bomb helped bring an end to World War II. Its legacy of peaceful uses of atomic energy continues to have an impact on history and science.\n",
        //                    "2\tEssay on The Manhattan Project - The Manhattan Project The Manhattan Project was to see if making an atomic bomb possible. The success of this project would forever change the world forever making it known that something this powerful can be manmade.",
        //                    "3\tThe Manhattan Project was the name for a project conducted during World War II, to develop the first atomic bomb. It refers specifically to the period of the project from 194 Ã¢Â€Â¦ 2-1946 under the control of the U.S. Army Corps of Engineers, under the administration of General Leslie R. Groves.",
        //                    "4\tversions of each volume as well as complementary websites. The first websiteÃ¢Â€Â“The Manhattan Project: An Interactive HistoryÃ¢Â€Â“is available on the Office of History and Heritage Resources website, http://www.cfo. doe.gov/me70/history. The Office of History and Heritage Resources and the National Nuclear Security",
        //                    "5\tThe Manhattan Project. This once classified photograph features the first atomic bomb Ã¢Â€Â” a weapon that atomic scientists had nicknamed Gadget.. The nuclear age began on July 16, 1945, when it was detonated in the New Mexico desert."};

        //for (String line: lines) 
        
        TextualFileManager txt = new TextualFileManager("C:\\progettiGitHub\\search-engine\\test-collection20000.tsv", MODE.READ);

        String line;

        while((line=txt.readLine()) != null){

            Pair<Integer, String> lineFormatted = null;

            try {
                lineFormatted = Preprocessor.parseLine(line);
            } catch (IllegalArgumentException e){
                e.printStackTrace();
                continue;
            }

            int docNo = lineFormatted.getKey();
            String docText = lineFormatted.getValue();

            processDocument(docText, docIdCounter, docNo);
            docIdCounter += 1;
        }

        //System.out.print(indexManager.toString());
    }

    /**
     * this method is in charge to store as file in a block:
     * - the vocabulary generated during the processing of the docs of the current block
     * - the document index for the docs of the current block
     * - the invertedIndex for the current block
     * The fields for the vocabulary stored on file also need the offset of the inverted index for the posting list of each term,
     * then we have to process the inverted index before the vocabulary
     */
    private void saveBlock(){
        System.out.println("Saving block...");

        // here we need the block number for the current block to load the block managers
        InvertedIndexBlockManager invertedIndexBlockManager = null;
        VocabularyBlockManager vocabularyBlockManager = null;
        DocumentIndexBlockManager documentIndexBlockManager = null;
        try {
            invertedIndexBlockManager = new InvertedIndexBlockManager(Indexer.currentBlockNo);
            vocabularyBlockManager = new VocabularyBlockManager(Indexer.currentBlockNo);
            documentIndexBlockManager = new DocumentIndexBlockManager(Indexer.currentBlockNo);
            // the same for the other managers...
        } catch (Exception e) {
            e.printStackTrace();
            // here we must stop the execution
            System.exit(-1);
        }
        
        

        // the first thing to do is to sort the Index by term lexicographically
        ArrayList<String> terms = this.indexManager.getSortedKeys();

        int invertedIndexOffset = 0;

        // then we can iterate over the index terms one at a time 
        for (String term : terms) {
            IndexRecord record = this.indexManager.getRecord(term);
        
            // store the posting list of that term in the inverted index
            ArrayList<Posting> postings = record.getPostingList();

            invertedIndexBlockManager.writeRow(postings);

            // once that the posting list is saved, we have the length of it on file
            int currentPostingListLen = ( ( postings.size() ) * 2) ;    // each element is made of two integers

            // we can now save an entry in the vocabulary with docid, df, cf, offset, docno
            VocabularyFileRecord vocabularyRecord = new VocabularyFileRecord(term, record.getCf(), record.getDf(), invertedIndexOffset);

            vocabularyBlockManager.writeRow(vocabularyRecord);

            // we can use the offset of the previous posting list + the length of the previous posting list 
            // to obtain the offset of the next posting list
            invertedIndexOffset += currentPostingListLen;
        }
        
        // in parallel with the creation of the vocabulary and of the inverted index, we can build the documentIndex
        // TODO: parallelize maybe
        ArrayList<DocumentIndexFileRecord> documentIndexList = this.documentIndex.getSortedList();
        for (DocumentIndexFileRecord documentIndexFileRecord : documentIndexList) {
            documentIndexBlockManager.writeRow(documentIndexFileRecord);
        }



        // once the block is saved, we go to the next block, then we have to:
        // increase the blockNo
        Indexer.currentBlockNo += 1;

        // close block managers
        invertedIndexBlockManager.closeBlock();
        vocabularyBlockManager.closeBlock();
        documentIndexBlockManager.closeBlock();

        // reset data structures
        this.resetDataStructures();

        System.out.println("Done");

    }

    /*
        metodi:
            -   metodo leggere un doc alla volta (skippare quelli non validi) e eseguire operazioni di preprocessing
            -   metodo che prende l'output del modulo di preprocessing (lista di token) per aggiornare vocabolario,
                creare entry del document index e aggiornare posting list del termine
     */
}
