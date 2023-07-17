package common.manager.block;

import common.bean.Posting;

import java.io.IOException;
import java.util.ArrayList;

public class InvertedIndexBlockManager extends BinaryBlockManager<ArrayList<Posting>> {

    protected static String blockDirectory = "/data/output/invertedIndexBlocks/";

    public InvertedIndexBlockManager(int blockNo) throws IOException{
        super(blockNo, blockDirectory);
    }

    /**
     * @param ArrayList<Posting> r a complete posting list for a certain term
     */
    @Override
    public void writeRow(ArrayList<Posting> r) throws Exception{

        for (Posting posting : r) {
            this.binaryFileManager.writeInt(posting.getDocid());
            this.binaryFileManager.writeInt(posting.getFreq());
        }

        // when we are here, in the binary output file we have r.length * 2 integers
    }
    
}