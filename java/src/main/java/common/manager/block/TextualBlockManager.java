package common.manager.block;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import java.nio.file.Paths;

import common.manager.file.BinaryFileManager;
import common.manager.file.FileManager.MODE;
import common.manager.file.TextualFileManager;

public abstract class TextualBlockManager<T> implements BlockManager<T> {

    protected int blockNo;
    protected TextualFileManager textualFileManager;
    protected String blockPath = null;


    public TextualBlockManager(int blockNo, String blockDirectory, MODE mode) throws IOException{
        if( ! ( new File(blockDirectory)).exists() ){
            Files.createDirectories(Paths.get(blockDirectory));
        }
        this.blockNo = blockNo;
        this.blockPath = blockDirectory + this.blockNo + ".txt";

        if (mode == MODE.WRITE){
            this.openNewBlock();
        } else if (mode == MODE.READ){
            this.openBlock();
        }
    }

    public TextualBlockManager(String blockName, String blockDirectory, MODE mode) throws IOException {
        if( ! ( new File(blockDirectory)).exists() ){
            Files.createDirectories(Paths.get(blockDirectory));
        }
        this.blockPath = blockDirectory + blockName + ".txt";

        if (mode == MODE.WRITE){
            this.openNewBlock();
        } else if (mode == MODE.READ){
            this.openBlock();
        }
    }

    protected void openBlock() throws IOException {
        File f = new File(this.blockPath);
        if(!f.exists()) {
            throw new IOException("file doesn't exist");
        }
        this.textualFileManager = new TextualFileManager(this.blockPath, MODE.READ, "UTF-8");
    };

    protected void openNewBlock() throws IOException {
        File f = new File(this.blockPath);
        if (f.exists()) {
            // Delete the existing folders
            emptyPath(f);
        }
        this.textualFileManager = new TextualFileManager(this.blockPath, MODE.WRITE);
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
            throw new IOException("Failed to delete file: " + file.getAbsolutePath());
        }
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
