package indexing;

import common.bean.*;
import common.manager.CollectionStatisticsManager;
import common.manager.block.DocumentIndexBlockManager;
import common.manager.block.SkipBlockBlockManager;
import common.manager.block.SplittedInvertedIndexBlockManager;
import common.manager.block.VocabularyBlockManager;
import common.manager.file.TextualFileManager;
import common.manager.file.FileManager.MODE;
import config.ConfigLoader;
import indexing.manager.IndexManager;
import indexing.manager.IndexManager.IndexRecord;
import javafx.util.Pair;
import preprocessing.Preprocessor;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class Indexer {

    static final private String inputCollection = ConfigLoader.getProperty("data.collection.path");
    static final private String charsetEncoding = ConfigLoader.getProperty("data.charset");
    static final private String collectionStatisticsFilePath = ConfigLoader.getProperty("collectionStatistics.filePath");

    static final private boolean useCompression = ConfigLoader.getPropertyBool("invertedIndex.useCompression");

    static final private int memoryOccupationThreshold = ConfigLoader.getIntProperty("memory.threshold");
    static private int docIdCounter = 0;
    static private int currentBlockNo = 0;

    private DocumentIndex documentIndex;
    private IndexManager indexManager;
    private CollectionStatisticsManager collectionStatisticsManager;
    private CollectionStatistics collectionStatistics;

    public class TermBlockListComparator implements Comparator<Pair<String, Integer>> {
        @Override
        public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
            int keyComparison = o1.getKey().compareTo(o2.getKey());
            if (keyComparison == 0) {
                // if keys are equal, so compare based on Integer values
                return Integer.compare(o1.getValue(), o2.getValue());
            } else {
                // if keys are different, use the result of the key comparison
                return keyComparison;
            }
        }
    }

    public Indexer(){
        this.documentIndex = new DocumentIndex();
        this.indexManager = new IndexManager();
        this.collectionStatistics = new CollectionStatistics(0, 0);
        this.collectionStatisticsManager = new CollectionStatisticsManager(collectionStatisticsFilePath);
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

        // Update Document Index, Vocabulary, InvertedIndex and Collection Statistics
        int totalDocLength = terms.length;

        documentIndex.addInformation(totalDocLength, docId, docNo);
        indexManager.addPostings(docId, termCounter);

        int currentTotalDocs = collectionStatistics.getTotalDocuments();
        double currentAvgDocLength = collectionStatistics.getAverageDocumentLength();
        int newTotalDocs = currentTotalDocs + 1;
        double newAvgDocLength = ((currentAvgDocLength * currentTotalDocs) + totalDocLength) / newTotalDocs;
        collectionStatistics.setTotalDocuments(newTotalDocs);
        collectionStatistics.setAverageDocumentLength(newAvgDocLength);
    }

    public void processCorpus(){
        //collection.tar.gz
        TextualFileManager txt = new TextualFileManager(inputCollection, MODE.READ, charsetEncoding);

        String line;

        while((line=txt.readLine()) != null){

            //System.out.print("\rprocessing line " + docIdCounter + "\t memory usage: " + getMemoryUsage(false));

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
        //once process corpus is done, I can save CollectionStatistics on file
        collectionStatisticsManager.saveCollectionStatistics(collectionStatistics);
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
        SplittedInvertedIndexBlockManager invertedIndexBlockManager = null;
        VocabularyBlockManager vocabularyBlockManager = null;
        DocumentIndexBlockManager documentIndexBlockManager = null;
        SkipBlockBlockManager skipBlockBlockManager = null;
        try {
            invertedIndexBlockManager = new SplittedInvertedIndexBlockManager(Indexer.currentBlockNo, MODE.WRITE, useCompression);
            vocabularyBlockManager = new VocabularyBlockManager(Indexer.currentBlockNo, MODE.WRITE);
            documentIndexBlockManager = new DocumentIndexBlockManager(Indexer.currentBlockNo, MODE.WRITE);
            skipBlockBlockManager = new SkipBlockBlockManager(Indexer.currentBlockNo, MODE.WRITE);
        } catch (Exception e) {
            e.printStackTrace();
            // here we must stop the execution
            System.exit(-1);
        }
        
        

        // the first thing to do is to sort the Index by term lexicographically
        ArrayList<String> terms = this.indexManager.getSortedKeys();

        int skipBlockOffset = 0;

        // then we can iterate over the index terms one at a time 
        for (String term : terms) {
            IndexRecord record = this.indexManager.getRecord(term);
        
            // store the posting list of that term in the inverted index
            ArrayList<Posting> postings = record.getPostingList();

            //if(term.equals("atom")){
            //    System.out.println("durante la saveBlock\t" + term + ": \t" + postings.toString());
            //}

            ArrayList<SkipBlock> skipBlocks = null;

            try {
                // once that the posting list is saved, we have the length of it on file
                skipBlocks = invertedIndexBlockManager.writeRowReturnSkipBlockInfos(postings);
            } catch (Exception e) {
                System.out.println("could not write row of the inverted index for the posting list of the term "+ term);
                e.printStackTrace();
                System.exit(-1);
            }
            try {
                for (SkipBlock sb : skipBlocks) {
                    
                        skipBlockBlockManager.writeRow(sb);
                        //if(term.equals("atom")){
                        //    System.out.println("saveblock sb: "+sb.toString());
                        //}
                }
            } catch (Exception e) {
                    e.printStackTrace();
            }
            
            // we can now save an entry in the vocabulary with docid, df, cf, offset, docno
            VocabularyFileRecord vocabularyRecord = new VocabularyFileRecord(term, record.getCf(), record.getDf(), skipBlockOffset, skipBlocks.size());

            vocabularyBlockManager.writeRow(vocabularyRecord);


            // skipBlocks.size() is the number of skip blocks of which the posting list of the current term is made
            skipBlockOffset += SkipBlock.SKIP_BLOCK_ENTRY_SIZE * skipBlocks.size();
        }


        
        // in parallel with the creation of the vocabulary and of the inverted index, we can build the documentIndex
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
        skipBlockBlockManager.closeBlock();

        System.out.println("Done! The block is made of " + terms.size() + " terms and " + documentIndexList.size() + " documents\tCurrentDocId: " + docIdCounter + "\tcurrent blockNo: " + currentBlockNo);

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
            } catch (IOException e) {
                e.printStackTrace();
            }
            mergeDocumentIndexBlock(documentIndexBlockManagerReader, documentIndexBlockManagerWriter);
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
        SplittedInvertedIndexBlockManager mergedInvertedIndexBlockManager = null;
        SkipBlockBlockManager mergedSkipBlockBlockManager = null;
        try {
            mergedVocabularyBlockManager = new VocabularyBlockManager( "merged-vocabulary", MODE.WRITE);
            mergedInvertedIndexBlockManager = new SplittedInvertedIndexBlockManager("merged-inverted-index", MODE.WRITE, useCompression);
            mergedSkipBlockBlockManager = new SkipBlockBlockManager("merged-skipblocks", MODE.WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // open vocabulary and inverted index block files
        VocabularyBlockManager[] arrayVocabularyManagers = new VocabularyBlockManager[currentBlockNo];
        SplittedInvertedIndexBlockManager[] arrayIndexManagers = new SplittedInvertedIndexBlockManager[currentBlockNo];
        SkipBlockBlockManager[] arraySkipBlockManagers = new SkipBlockBlockManager[currentBlockNo];

        VocabularyFileRecord[] vocabularyFileRecords = new VocabularyFileRecord[currentBlockNo];
        ArrayList<Pair<String, Integer>> termBlockList = new ArrayList<>();

        int skipBlockOffset = 0;


        for (int i = 0; i < currentBlockNo; i++) {
            try {
                arrayVocabularyManagers[i] = new VocabularyBlockManager(i, MODE.READ);
                vocabularyFileRecords[i] = arrayVocabularyManagers[i].readRow();
                if (vocabularyFileRecords[i] != null){
                    termBlockList.add(new Pair<String, Integer>(vocabularyFileRecords[i].getTerm(), i));
                }
                arrayIndexManagers[i] = new SplittedInvertedIndexBlockManager(i, MODE.READ, useCompression);
                arraySkipBlockManagers[i] = new SkipBlockBlockManager(i, MODE.READ);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        while (!termBlockList.isEmpty()) {
            // Use Comparator.comparing() and thenComparing() for sorting termBlockList
            termBlockList.sort(new TermBlockListComparator());

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
                int firstSkipBlockOffset = vocabularyFileRecords[blockId].getOffset();
                int howManySkipBlocks = vocabularyFileRecords[blockId].getHowManySkipBlocks();
                // TOCHECK: here we have to iterate howManySkipBlocks times to load 
                // howManySkipBlocks SkipBlocks objects using readRow method of SkipBlockBlockManager class 
                // and using as starting offset firstSkipBlockOffset
                // for each SkipBlock obtained, we have to invoke the method arrayIndexManagers[blockId].readRow() giving as parameter the skipBlock
                // all the arrayList of Posting returned by that readRow method, must be put in the postingList object
                //if(termLexMin.equals("atom")){
                //    System.out.println("howManySkipBlocks: "+howManySkipBlocks);
                //}
                for (int i = 0; i < howManySkipBlocks; i++) {
                    SkipBlock sb = null;
                    try {
                        sb = arraySkipBlockManagers[blockId].readRowAt(firstSkipBlockOffset + i * SkipBlock.SKIP_BLOCK_ENTRY_SIZE);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        System.exit(-1);
                    }
                    //if(termLexMin.equals("atom")){
                    //   System.out.println("sb: "+sb.toString());
                    //}
                    postingList.addAll(arrayIndexManagers[blockId].readRow(sb));
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
            ArrayList<SkipBlock> generatedSkipBlocks = null;
            try {
                // add the posting list to the resulting inverted index
                //if(termLexMin.equals("atom") || termLexMin.equals("tourism") || termLexMin.equals("tourist")){
                //    System.out.println("durante la mergeBlock\t" + termLexMin + ": \t" + postingList.toString());
                //}

                generatedSkipBlocks = mergedInvertedIndexBlockManager.writeRowReturnSkipBlockInfos(postingList);
                
                for (SkipBlock sb : generatedSkipBlocks) {
                    mergedSkipBlockBlockManager.writeRow(sb);
                }
                
                VocabularyFileRecord mergedVocabularyFileRecord = new VocabularyFileRecord(termLexMin, cf, df, skipBlockOffset, generatedSkipBlocks.size());
            
                mergedVocabularyBlockManager.writeRow(mergedVocabularyFileRecord);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            skipBlockOffset += SkipBlock.SKIP_BLOCK_ENTRY_SIZE * generatedSkipBlocks.size();

        }
        mergedInvertedIndexBlockManager.closeBlock();
        mergedVocabularyBlockManager.closeBlock();
        mergedSkipBlockBlockManager.closeBlock();


    }


    public CollectionStatistics getCollectionStatistics() {
        return collectionStatistics;
    }

}
