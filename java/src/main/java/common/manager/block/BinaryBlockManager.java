package common.manager.block;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import common.manager.file.BinaryFileManager;
import common.manager.file.FileManager.MODE;

public abstract class BinaryBlockManager<T> implements BlockManager<T> {

    protected int blockNo;
    protected BinaryFileManager binaryFileManager;
    protected String blockPath = null;




    public BinaryBlockManager(int blockNo, String blockDirectory) throws IOException{
        if( ! ( new File(blockDirectory)).exists() ){
            Files.createDirectories(Paths.get(blockDirectory));
        }
        this.blockNo = blockNo;
        this.blockPath = blockDirectory + this.blockNo + ".binary";
        this.openNewBlock();
    }


    protected void openNewBlock() throws IOException{
        File f = new File(this.blockPath);
        if(f.exists()) {
            throw new IOException("file already exists");
            // consider if it would be better to delete the old one instead of throwing the exception
        }
        this.binaryFileManager = new BinaryFileManager(this.blockPath, MODE.WRITE);
    }

    @Override
    public boolean closeBlock() {
        if(this.binaryFileManager == null){
            return false;
        }
        this.binaryFileManager.close();
        return true;
    }

}