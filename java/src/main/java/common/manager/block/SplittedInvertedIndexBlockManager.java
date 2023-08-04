package common.manager.block;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import common.bean.Posting;
import common.bean.SkipBlock;
import common.bean.WrittenBytes;
import common.manager.block.VocabularyBlockManager.OffsetType;
import common.manager.file.BinaryFileManager;
import common.manager.file.FileManager.MODE;
import config.ConfigLoader;

public class SplittedInvertedIndexBlockManager extends BinaryBlockManager<ArrayList<Posting>>{

    protected BinaryFileManager docIdBinaryFileManager;
    protected BinaryFileManager freqBinaryFileManager;

    protected String docIdsBlockPath;
    protected String freqsBlockPath;

    protected static final String docIdsBlockFolder = ConfigLoader.getProperty("blocks.invertedindex.docIdFilePath");
    protected static final String freqBlockFolder = ConfigLoader.getProperty("blocks.invertedindex.freqFilePath");

    public static int skipBlockMaxLength = ConfigLoader.getIntProperty("skipblocks.maxLen");

    public SplittedInvertedIndexBlockManager(int blockNo, MODE mode) throws IOException {
        this.mode = mode;
        this.blockNo = blockNo;
        this.binaryFileManager = null;  //we do not use this binaryFileManager for code clearness


        if( ! ( new File(SplittedInvertedIndexBlockManager.docIdsBlockFolder)).exists() ){
            Files.createDirectories(Paths.get(SplittedInvertedIndexBlockManager.docIdsBlockFolder));
        }

        if( ! ( new File(SplittedInvertedIndexBlockManager.freqBlockFolder)).exists() ){
            Files.createDirectories(Paths.get(SplittedInvertedIndexBlockManager.freqBlockFolder));
        }

        this.docIdsBlockPath = SplittedInvertedIndexBlockManager.docIdsBlockFolder + this.blockNo + ".binary";
        this.freqsBlockPath = SplittedInvertedIndexBlockManager.freqBlockFolder + this.blockNo + ".binary";

        if (mode == MODE.WRITE){
            this.openNewBlock();
        } else if (mode == MODE.READ){
            this.openBlock();
        }
    }

    public SplittedInvertedIndexBlockManager(String blockName, MODE mode) throws IOException {
        this.mode = mode;
        this.binaryFileManager = null;  //we do not use this binaryFileManager for code clearness

        if( ! ( new File(SplittedInvertedIndexBlockManager.docIdsBlockFolder)).exists() ){
            Files.createDirectories(Paths.get(SplittedInvertedIndexBlockManager.docIdsBlockFolder));
        }

        if( ! ( new File(SplittedInvertedIndexBlockManager.freqBlockFolder)).exists() ){
            Files.createDirectories(Paths.get(SplittedInvertedIndexBlockManager.freqBlockFolder));
        }

        this.docIdsBlockPath = SplittedInvertedIndexBlockManager.docIdsBlockFolder + blockName + ".binary";
        this.freqsBlockPath = SplittedInvertedIndexBlockManager.freqBlockFolder + blockName + ".binary";

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
        File f = new File(this.docIdsBlockPath);
        if (f.exists()) {
            // Delete the existing folders
            emptyPath(f);
        }
        //TODO: pass the compressor object to the constructor
        this.docIdBinaryFileManager = new BinaryFileManager(this.docIdsBlockPath, MODE.WRITE);

        f = new File(this.freqsBlockPath);
        if (f.exists()) {
            // Delete the existing folders
            emptyPath(f);
        }
        //TODO: pass the compressor object to the constructor
        this.freqBinaryFileManager = new BinaryFileManager(this.freqsBlockPath, MODE.WRITE);
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
        File f = new File(this.docIdsBlockPath);
        if(!f.exists()) {
            throw new IOException("file " + this.docIdsBlockPath + " doesn't exist");
        }
        this.docIdBinaryFileManager = new BinaryFileManager(this.docIdsBlockPath, MODE.READ);
        f = new File(this.freqsBlockPath);
        if(!f.exists()) {
            throw new IOException("file " + this.freqsBlockPath + " doesn't exist");
        }
        this.freqBinaryFileManager = new BinaryFileManager(this.freqsBlockPath, MODE.READ);
    }

    @Override
    public boolean closeBlock() {
        if(this.docIdBinaryFileManager == null || this.freqBinaryFileManager == null){
            return false;
        }
        this.docIdBinaryFileManager.close();
        this.freqBinaryFileManager.close();
        return true;
    }

    @Override
    public void writeRow(ArrayList<Posting> r) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'writeRow'");
    }

    /**
     * @param r<Posting> r a complete posting list for a certain term
     * @return ArrayList<SkipBlock> the length is be equal to the number of blocks in which the posting list is split
     */
    public ArrayList<SkipBlock> writeRowReturnSkipBlockInfos(ArrayList<Posting> r) throws Exception{
        ArrayList<SkipBlock> ret = new ArrayList<SkipBlock>();

        int counter = 0;
        int startingIndex = 0;
        long docIdOffset = this.docIdBinaryFileManager.getCurrentPosition();
        long freqOffset = this.docIdBinaryFileManager.getCurrentPosition();
        // here we have to split each posting of the posting array in two integers.
        // we must obtain two arrays of integers, each of them has to be stored on the dedicated file by the binaryfilemanager
        ArrayList<Integer> docIds = new ArrayList<Integer>();
        ArrayList<Integer> freqs = new ArrayList<Integer>();
        for (Posting posting : r) {
            docIds.add( posting.getDocid());
            freqs.add(posting.getFreq());
            counter+=1;
            if(counter % SplittedInvertedIndexBlockManager.skipBlockMaxLength == 0){
                // here we write a whole skip block on file and store its informations
                int docIdWrittenBytes = this.docIdBinaryFileManager.writeIntArray(docIds.subList(startingIndex, docIds.size()-1).stream().mapToInt(Integer::intValue).toArray());
                int freqWrittenBytes  = this.freqBinaryFileManager.writeIntArray(freqs.subList(startingIndex, docIds.size()-1).stream().mapToInt(Integer::intValue).toArray());

                SkipBlock sb = new SkipBlock(docIdOffset, freqOffset, docIds.get(counter - 1), counter - startingIndex, docIdWrittenBytes, freqWrittenBytes);
                ret.add(sb);
                
                startingIndex += SplittedInvertedIndexBlockManager.skipBlockMaxLength;
                docIdOffset += docIdWrittenBytes;
                freqOffset  += freqWrittenBytes;
            }
        }

        int docIdWrittenBytes = this.docIdBinaryFileManager.writeIntArray(docIds.subList(startingIndex, docIds.size()-1).stream().mapToInt(Integer::intValue).toArray());
        int freqWrittenBytes  = this.freqBinaryFileManager.writeIntArray(freqs.subList(startingIndex, docIds.size()-1).stream().mapToInt(Integer::intValue).toArray());

        SkipBlock sb = new SkipBlock(docIdOffset, freqOffset, docIds.get(counter - 1), counter - startingIndex, docIdWrittenBytes, freqWrittenBytes);
        ret.add(sb);
        
        //startingIndex += SplittedInvertedIndexBlockManager.skipBlockMaxLength;
        //docIdOffset += docIdWrittenBytes;
        //freqOffset  += freqWrittenBytes;

        return ret;
    }
    
    @Override
    public ArrayList<Posting> readRow() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'readRow'");
    }
    /**
     * returns the PostingList of a specified skipBlock
     * @param sb
     * @return
     */
    public ArrayList<Posting> readRow(SkipBlock sb){
        try {
            this.docIdBinaryFileManager.seek(sb.getDocIdFileOffset());
            this.freqBinaryFileManager.seek(sb.getFreqFileOffset());
        } catch (Exception e) {
            e.printStackTrace();
        }
        int[] docIds = new int[0];
        int[] freqs = new int[0];

        try {
            docIds = this.docIdBinaryFileManager.readIntArray(sb.getDocIdByteSize(), sb.getHowManyPostings());
            freqs = this.freqBinaryFileManager.readIntArray(sb.getFreqByteSize(), sb.getHowManyPostings());
        } catch (Exception e) {
            e.printStackTrace();
        }

        ArrayList<Posting> ret = new ArrayList<Posting>();

        for (int i = 0; i < sb.getHowManyPostings(); i++) {
            ret.add(new Posting(docIds[i], freqs[i]));
        }

        return ret;
    }


}
