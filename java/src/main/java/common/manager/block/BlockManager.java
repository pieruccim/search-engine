package common.manager.block;

import java.io.IOException;

public interface BlockManager<T> {

    static String blockPath = null;


    //protected abstract void openNewBlock() throws IOException;
        // check if the block already exists, in that case throws an exception

        // open in write mode the file where to store data of the current block


    public abstract void writeRow(T r);
        // receives the record to be written in the block, which can either be:
        // - a posting list
        // - a vocabulary row
        // - a document index record

        // then it writes it on disk according to the chosen encoding for the type of block


    public abstract boolean closeBlock();
        // check if the block was already closed

        // then tries to close the block from write mode 


}
