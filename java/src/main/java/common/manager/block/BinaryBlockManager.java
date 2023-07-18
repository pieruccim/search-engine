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


    public BinaryBlockManager(int blockNo, String blockDirectory, MODE mode) throws IOException{
        if( ! ( new File(blockDirectory)).exists() ){
            Files.createDirectories(Paths.get(blockDirectory));
        }
        this.blockNo = blockNo;
        this.blockPath = blockDirectory + this.blockNo + ".binary";
        if (mode == MODE.WRITE){
            this.openNewBlock();
        } else if (mode == MODE.READ){
            this.openBlock();
        }
    }

    public BinaryBlockManager(String blockName, String blockDirectory, MODE mode) throws IOException {
        if( ! ( new File(blockDirectory)).exists() ){
            Files.createDirectories(Paths.get(blockDirectory));
        }
        this.blockPath = blockDirectory + blockName + ".binary";
        if (mode == MODE.WRITE){
            this.openNewBlock();
        } else if (mode == MODE.READ){
            this.openBlock();
        }
    }

    protected void openNewBlock() throws IOException{
        File f = new File(this.blockPath);
        if(f.exists()) {
            throw new IOException("file already exists");
            // consider if it would be better to delete the old one instead of throwing the exception
        }
        this.binaryFileManager = new BinaryFileManager(this.blockPath, MODE.WRITE);
    }

    protected void openBlock() throws IOException {
        File f = new File(this.blockPath);
        if(!f.exists()) {
            throw new IOException("file doesn't exist");
        }
        this.binaryFileManager = new BinaryFileManager(this.blockPath, MODE.READ);
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
