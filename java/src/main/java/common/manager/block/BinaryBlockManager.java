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

    protected MODE mode;


    public BinaryBlockManager(int blockNo, String blockDirectory, MODE mode) throws IOException{
        if( ! ( new File(blockDirectory)).exists() ){
            Files.createDirectories(Paths.get(blockDirectory));
        }
        this.mode = mode;
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
        this.mode = mode;
        if (mode == MODE.WRITE){
            this.openNewBlock();
        } else if (mode == MODE.READ){
            this.openBlock();
        }
    }

    protected void openNewBlock() throws IOException {
        if(this.mode != MODE.WRITE){
            throw new IOException("Cannot open new block since BlockManager mode is not MODE.WRITE\tcurrent mode: " + this.mode);
        }
        File f = new File(this.blockPath);
        if (f.exists()) {
            // Delete the existing folders
            emptyPath(f);
        }

        this.binaryFileManager = new BinaryFileManager(this.blockPath, MODE.WRITE);
    }

    private void emptyPath(File file) throws IOException {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File subFile : files) {
                    emptyPath(subFile);
                }
            }
        }

        if (!file.delete()) {
            throw new IOException("Failed to delete file or directory: " + file.getAbsolutePath());
        }
    }


    protected void openBlock() throws IOException {
        if(this.mode != MODE.READ){
            throw new IOException("Cannot open existing block in read mode since BlockManager mode is not MODE.READ\tcurrent mode: " + this.mode);
        }
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

    public String getBlockPath() {
        return blockPath;
    }
}
