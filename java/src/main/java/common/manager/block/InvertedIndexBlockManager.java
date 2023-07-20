package common.manager.block;

import common.bean.DocumentIndexFileRecord;
import common.bean.Posting;
import common.manager.file.FileManager;
import config.ConfigLoader;
import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;

public class InvertedIndexBlockManager extends BinaryBlockManager<ArrayList<Posting>> {

    protected static String blockDirectory = ConfigLoader.getProperty("blocks.invertedIndex.path");

    public InvertedIndexBlockManager(int blockNo, FileManager.MODE mode) throws IOException{
        super(blockNo, blockDirectory, mode);
    }

    public InvertedIndexBlockManager(String blockName, FileManager.MODE mode) throws IOException{
        super(blockName, blockDirectory, mode);
    }

    /**
     * @param r<Posting> r a complete posting list for a certain term
     */
    @Override
    public void writeRow(ArrayList<Posting> r) throws Exception{

        for (Posting posting : r) {
            this.binaryFileManager.writeInt(posting.getDocid());
            this.binaryFileManager.writeInt(posting.getFreq());
        }

        // when we are here, in the binary output file we have r.length * 2 integers
    }

    @Override
    public ArrayList<Posting> readRow() throws Exception {
        return null;
    }

    /**
     * 
     * @param offset from the beginning of the block file to read from as number of integers (of 4 bytes)
     * @param numPostings how many posting have to be read (each posting is a pair of integers (docID, freq))
     * @return ArrayList<Posting> the List of postings retrieved
     * @throws Exception
     */
    public ArrayList<Posting> readRow(int offset, int numPostings) throws Exception {

        ArrayList<Posting> postingList = new ArrayList<>();

        for(int i = 0; i < numPostings; i++) {
            Pair<Integer, Integer> docIdFreq = readCouple(offset + i * 2);
            if(docIdFreq == null){
                break;
            }
            Posting posting = new Posting(docIdFreq.getKey(), docIdFreq.getValue());
            postingList.add(posting);
        }

        return postingList;
    }

    public Pair<Integer, Integer> readCouple(int offset){
        int docId = 0;
        int freq = 0;

        try {
            docId = binaryFileManager.readInt(offset);
            freq = binaryFileManager.readInt();
        } catch (Exception e){
            return null;
        }

        return new Pair<>(docId,freq);
    }

}
