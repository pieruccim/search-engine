package common.manager.block;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import java.nio.file.Paths;

import common.manager.file.FileManager.MODE;
import common.manager.file.TextualFileManager;

public abstract class TextualBlockManager<T> implements BlockManager<T> {

    protected int blockNo;
    protected TextualFileManager textualFileManager;
    protected String blockPath = null;




    public TextualBlockManager(int blockNo, String blockDirectory) throws IOException{
        if( ! ( new File(blockDirectory)).exists() ){
            Files.createDirectories(Paths.get(blockDirectory));
        }
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
