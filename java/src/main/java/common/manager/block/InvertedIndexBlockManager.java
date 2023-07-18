package common.manager.block;

import common.bean.DocumentIndexFileRecord;
import common.bean.Posting;
import common.manager.file.FileManager;
import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;

public class InvertedIndexBlockManager extends BinaryBlockManager<ArrayList<Posting>> {

    protected static String blockDirectory = System.getProperty("user.dir") + "/src/main/java/data/output/invertedIndexBlocks/";

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

    public ArrayList<Posting> readRow(int offset, int numPostings) throws Exception {

        ArrayList<Posting> postingList = new ArrayList<>();

        for(int i = 0; i < numPostings; i++) {
            Pair<Integer, Integer> docIdFreq = readCouple(offset + i);
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
