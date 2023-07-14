package common.manager.block;

import java.io.File;
import java.io.IOException;

import common.manager.file.FileManager.MODE;
import common.manager.file.TextualFileManager;

public abstract class TextualBlockManager<T> implements BlockManager<T> {

    protected int blockNo;
    protected TextualFileManager textualFileManager;
    protected String blockPath = null;
    
    protected static String blockDirectory = null;



    public TextualBlockManager(int blockNo) throws IOException{
        this.blockNo = blockNo;
        this.blockPath = blockDirectory + this.blockNo + ".txt";
        this.openNewBlock();
    }

    protected void openNewBlock() throws IOException{
        File f = new File(this.blockPath);
        if(f.exists()) {
            throw new IOException("file already exists");
            // consider if it would be better to delete the old one instead of throwing the exception
        }
        this.textualFileManager = new TextualFileManager(this.blockPath, MODE.WRITE);
    }

    @Override
    public boolean closeBlock() {
        if(this.textualFileManager == null){
            return false;
        }
        this.textualFileManager.close();
        return true;
    }
    
}
