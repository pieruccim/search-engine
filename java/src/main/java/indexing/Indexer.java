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
import indexing.manager.IndexManager;
import indexing.manager.IndexManager.IndexRecord;
import javafx.util.Pair;
import preprocessing.Preprocessor;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
        // C:\\Users\\nello\\Documents\\Intellij Projects\\search-engine\\test-collection20000.tsv
        // 
        TextualFileManager txt = new TextualFileManager("C:\\programmazione\\search-engine\\collection.tar.gz", MODE.READ, "UTF-8");

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

            //if(term.equals("manhattan")){
            //    System.out.println("durante la saveBlock\tmanhattan: \t" + postings.toString());
            //}

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

        int howMany;

        for (int i = 0; i < currentBlockNo; i++) {

            try {
                documentIndexBlockManagerReader = new DocumentIndexBlockManager(i, MODE.READ);
                documentIndexBlockManagerReader.checkDebug("inside try block of the constructor");
            } catch (IOException e) {
                e.printStackTrace();
            }
            documentIndexBlockManagerReader.checkDebug("before calling merge document index");
            howMany = mergeDocumentIndexBlock(documentIndexBlockManagerReader, documentIndexBlockManagerWriter);
            //System.out.println("Merged " + howMany + " read records from the block " + documentIndexBlockManagerReader.getBlockPath());
            documentIndexBlockManagerReader.checkDebug("after method mergeDocumentIndexBlock");
            documentIndexBlockManagerReader.closeBlock();
        }
        // Close file manager
        documentIndexBlockManagerWriter.closeBlock();
    }

    /**
     * reads all the rows of the given documentIndexBlockManagerReader until the end of the file
     * writes the read rows to the given documentIndexBlockManagerWriter
     * @return int the amount of rows read
     */
    private int mergeDocumentIndexBlock(DocumentIndexBlockManager documentIndexBlockManagerReader,
                                         DocumentIndexBlockManager documentIndexBlockManagerWriter){
        DocumentIndexFileRecord record;
        int howMany = 0;
        try {
            while ((record = documentIndexBlockManagerReader.readRow()) != null) {
                // System.out.println("docId: " + record.getDocId() + ", docNo: " + record.getDocNo() + ", len: " + record.getLen());
                documentIndexBlockManagerWriter.writeRow(record);
                howMany += 1;
            }
        } catch (Exception e) {
            e.printStackTrace(); // handle the exception appropriately
        }
        return howMany;
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

        VocabularyFileRecord[] vocabularyFileRecords = new VocabularyFileRecord[currentBlockNo];
        ArrayList<Pair<String, Integer>> termBlockList = new ArrayList<>();

        int mergedPostingListOffset = 0;

        for (int i = 0; i < currentBlockNo; i++) {
            try {
                arrayVocabularyManagers[i] = new VocabularyBlockManager(i, MODE.READ);
                vocabularyFileRecords[i] = arrayVocabularyManagers[i].readRow();
                if (vocabularyFileRecords[i] != null){
                    termBlockList.add(new Pair<String, Integer>(vocabularyFileRecords[i].getTerm(), i));
                }
                arrayIndexManagers[i] = new InvertedIndexBlockManager(i, MODE.READ);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        while (!termBlockList.isEmpty()) {
            termBlockList.sort(new Comparator<Pair<String, Integer>>() {
                @Override
                public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {

                    int val = o1.getKey().compareTo(o2.getKey());

                    switch (val) {
                        case 0:
                            if (o1.getValue() > o2.getValue()) {
                                return 1;
                            } else if (o1.getValue() < o2.getValue()) {
                                return -1;
                            } else {
                                System.out.println("Found two identical pairs: " + o1.getKey() + "-" + o1.getValue());
                                return 0;
                            }

                        default:
                            return val;

                    }

                }
            });


            // block of the min lexicographic term between all first terms of each block
            String termLexMin = termBlockList.get(0).getKey();

            //int index = 0;
            int cf = 0;
            int df = 0;
            ArrayList<Posting> postingList = new ArrayList<>();
            while ( ! termBlockList.isEmpty() ) {
                Pair<String, Integer> pair = termBlockList.get(0);
                if (!pair.getKey().equals(termLexMin)) {
                    break;
                }
                int blockId = pair.getValue();
                cf += vocabularyFileRecords[blockId].getCf();
                int tmpDf = vocabularyFileRecords[blockId].getDf();
                df += tmpDf;
                int tmpOffset = vocabularyFileRecords[blockId].getOffset();
                try {
                    postingList.addAll(arrayIndexManagers[blockId].readRow(tmpOffset, tmpDf));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                try {
                    vocabularyFileRecords[blockId] = arrayVocabularyManagers[blockId].readRow();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                termBlockList.remove(0);
                if (vocabularyFileRecords[blockId] != null) {
                    termBlockList.add(new Pair<String, Integer>(vocabularyFileRecords[blockId].getTerm(), blockId));
                }else{
                    // if the vocabulary for the block blockID is finished, we have to close its managers
                    arrayVocabularyManagers[blockId].closeBlock();
                    arrayIndexManagers[blockId].closeBlock();
                }
            }
            VocabularyFileRecord mergedVocabularyFileRecord = new VocabularyFileRecord(termLexMin, cf, df, mergedPostingListOffset);
            mergedPostingListOffset += postingList.size() * 2;

            try {
                // add the posting list to the resulting inverted index
                //if(termLexMin.equals("manhattan")){
                //    System.out.println("durante la mergeBlock\tmanhattan: \t" + postingList.toString());
                //}
                mergedInvertedIndexBlockManager.writeRow(postingList);
                mergedVocabularyBlockManager.writeRow(mergedVocabularyFileRecord);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
        mergedInvertedIndexBlockManager.closeBlock();
        mergedVocabularyBlockManager.closeBlock();


    }

}
