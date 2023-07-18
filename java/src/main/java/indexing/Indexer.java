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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class Indexer {

    static final private int memoryOccupationThreshold = 75;
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
            System.out.println("Memory occupation percentage: " + Math.round(memoryUsagePercentage * 100.0f) / 100.0f + "%" + "\t| used memory: " + usedMemoryBytes);
        }
        return memoryUsagePercentage;
    }

    private static boolean shouldStoreBlock(){
        return getMemoryUsage(false) >= memoryOccupationThreshold;
    }


    private void resetDataStructures(){
        documentIndex.reset();
        indexManager.reset();
        // Force garbage collector to free memory
        System.gc();
    }

    private void processDocument(String docText, int docId, int docNo){

        // Manage saving of the Block to disk when memory is above threshold (SPiMI Algorithm)
        if (shouldStoreBlock()){
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
        //collection.tar.gz     //test-collection20000.tsv
        TextualFileManager txt = new TextualFileManager("C:\\progettiGitHub\\search-engine\\test-collection10.tsv", MODE.READ, "UTF-8");

        String line;

        while((line=txt.readLine()) != null){

            Pair<Integer, String> lineFormatted = null;

            try {
                lineFormatted = Preprocessor.parseLine(line);
            } catch (IllegalArgumentException e){
                System.out.println("Cannot parse the line: \t"+line+"\n\n");
                e.printStackTrace();
                //System.exit(-1);
                continue;
            }

            int docNo = lineFormatted.getKey();
            String docText = lineFormatted.getValue();

            processDocument(docText, docIdCounter, docNo);
            docIdCounter += 1;
        }

        saveBlock();
        resetDataStructures();

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
            invertedIndexBlockManager = new InvertedIndexBlockManager(Indexer.currentBlockNo, MODE.WRITE);
            vocabularyBlockManager = new VocabularyBlockManager(Indexer.currentBlockNo, MODE.WRITE);
            documentIndexBlockManager = new DocumentIndexBlockManager(Indexer.currentBlockNo, MODE.WRITE);
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

            try {
                invertedIndexBlockManager.writeRow(postings);
            } catch (Exception e) {
                System.out.println("could not write row of the inverted index for the posting list of the term "+ term);
                e.printStackTrace();
                System.exit(-1);
            }

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
        try {
            for (DocumentIndexFileRecord documentIndexFileRecord : documentIndexList) {
                documentIndexBlockManager.writeRow(documentIndexFileRecord);
            }
        } catch (Exception e) {
            System.out.println("Could not store the document index row");
            e.printStackTrace();
            System.exit(-1);
        }
        



        // once the block is saved, we go to the next block, then we have to:
        // increase the blockNo
        Indexer.currentBlockNo += 1;

        // close block managers
        invertedIndexBlockManager.closeBlock();
        vocabularyBlockManager.closeBlock();
        documentIndexBlockManager.closeBlock();

        // reset data structures
        //this.resetDataStructures();
        System.out.println("The block is made of " + terms.size() + " terms and " + documentIndexList.size() + " documents");
        System.out.println("Done");

    }

    /**
     * This method is in charge of:
     *  -   opening all blocks
     *  -   loading to memory first term of each block
     *  -   sorting the terms in a lexicographic manner
     *  -   saving the first one to a new file (merged block)
     */

    public void mergeDataStructures(){
        mergeDocumentIndex();
        mergeInvertedIndex();
        // TODO: implement merging functions for Inverted Index and Vocabulary
    }

    private void mergeDocumentIndex(){

        DocumentIndexBlockManager documentIndexBlockManagerReader = null;
        DocumentIndexBlockManager documentIndexBlockManagerWriter = null;

        try {
             documentIndexBlockManagerWriter = new DocumentIndexBlockManager("merged-document-index", MODE.WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < currentBlockNo; i++) {

            try {
                documentIndexBlockManagerReader = new DocumentIndexBlockManager(i, MODE.READ);
            } catch (IOException e) {
                e.printStackTrace();
            }
            mergeDocumentIndexBlock(documentIndexBlockManagerReader, documentIndexBlockManagerWriter);
        }
        // Close file managers
        documentIndexBlockManagerWriter.closeBlock();
        documentIndexBlockManagerReader.closeBlock();
    }

    private void mergeDocumentIndexBlock(DocumentIndexBlockManager documentIndexBlockManagerReader,
                                         DocumentIndexBlockManager documentIndexBlockManagerWriter){
        DocumentIndexFileRecord record;
        try {
            while ((record = documentIndexBlockManagerReader.readRow()) != null) {
                // System.out.println("docId: " + record.getDocId() + ", docNo: " + record.getDocNo() + ", len: " + record.getLen());
                documentIndexBlockManagerWriter.writeRow(record);
            }
        } catch (Exception e) {
            e.printStackTrace(); // handle the exception appropriately
        }
    }

    private void mergeInvertedIndex(){

        // new files for merged vocabulary and inverted index
        VocabularyBlockManager mergedVocabularyBlockManager = null;
        InvertedIndexBlockManager mergedInvertedIndexBlockManager = null;
        try {
            mergedVocabularyBlockManager = new VocabularyBlockManager( "merged-vocabulary", MODE.WRITE);
            mergedInvertedIndexBlockManager = new InvertedIndexBlockManager("merged-inverted-index", MODE.WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // open vocabulary and inverted index block files
        VocabularyBlockManager[] arrayVocabularyManagers = new VocabularyBlockManager[currentBlockNo];
        InvertedIndexBlockManager[] arrayIndexManagers = new InvertedIndexBlockManager[currentBlockNo];

        VocabularyFileRecord[] fileRecords = new VocabularyFileRecord[currentBlockNo];
        HashMap<String, Integer> termBlock = new HashMap<>();

        for (int i = 0; i < currentBlockNo; i++) {
            try {
                arrayVocabularyManagers[i] = new VocabularyBlockManager(i, MODE.READ);
                fileRecords[i] = arrayVocabularyManagers[i].readRow();
                termBlock.put(fileRecords[i].getTerm(), i);
                arrayIndexManagers[i] = new InvertedIndexBlockManager(i, MODE.READ);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        ArrayList<String> keys = new ArrayList<>(termBlock.keySet());
        Collections.sort(keys);

        int block = termBlock.get(keys.get(0));

    }

    /*
        metodi:
            -   metodo leggere un doc alla volta (skippare quelli non validi) e eseguire operazioni di preprocessing
            -   metodo che prende l'output del modulo di preprocessing (lista di token) per aggiornare vocabolario,
                creare entry del document index e aggiornare posting list del termine
     */
}
