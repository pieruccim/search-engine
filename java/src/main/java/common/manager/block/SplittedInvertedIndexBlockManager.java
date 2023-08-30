package common.manager.block;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import common.bean.Posting;
import common.bean.SkipBlock;
import common.manager.file.BinaryFileManager;
import common.manager.file.FileManager.MODE;
import common.manager.file.compression.DeltaCompressor;
import common.manager.file.compression.UnaryCompressor;
import config.ConfigLoader;

public class SplittedInvertedIndexBlockManager extends BinaryBlockManager<ArrayList<Posting>>{

    protected BinaryFileManager docIdBinaryFileManager;
    protected BinaryFileManager freqBinaryFileManager;

    protected String docIdsBlockPath;
    protected String freqsBlockPath;

    protected boolean useCompression = true;

    protected static final String docIdsBlockFolder = ConfigLoader.getProperty("blocks.invertedindex.docIdFilePath");
    protected static final String freqBlockFolder = ConfigLoader.getProperty("blocks.invertedindex.freqFilePath");

    public static final int skipBlockMaxLength = ConfigLoader.getIntProperty("skipblocks.maxLen");
    private static final boolean useVariableBlockSize = ConfigLoader.getPropertyBool("skipblocks.size.variable.enabled");

    public SplittedInvertedIndexBlockManager(int blockNo, MODE mode, boolean useCompression) throws IOException {
        this.useCompression = useCompression;
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

    public SplittedInvertedIndexBlockManager(int blockNo, MODE mode) throws IOException {
        this(blockNo, mode, true);
    }

    public SplittedInvertedIndexBlockManager(String blockName, MODE mode, boolean useCompression) throws IOException {
        this.useCompression = useCompression;
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
    public SplittedInvertedIndexBlockManager(String blockName, MODE mode) throws IOException {
        this(blockName, mode, true);
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
        
        
        //System.out.println(this.freqsBlockPath);
        f = new File(this.freqsBlockPath);
        if (f.exists()) {
            // Delete the existing folders
            emptyPath(f);
        }
        if(this.useCompression){
            this.docIdBinaryFileManager = new BinaryFileManager(this.docIdsBlockPath, MODE.WRITE, new DeltaCompressor());
            this.freqBinaryFileManager = new BinaryFileManager(this.freqsBlockPath, MODE.WRITE, new UnaryCompressor());
        }else{
            this.docIdBinaryFileManager = new BinaryFileManager(this.docIdsBlockPath, MODE.WRITE);
            this.freqBinaryFileManager = new BinaryFileManager(this.freqsBlockPath, MODE.WRITE);
        }
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
        
        f = new File(this.freqsBlockPath);
        if(!f.exists()) {
            throw new IOException("file " + this.freqsBlockPath + " doesn't exist");
        }
        if(this.useCompression){
            this.docIdBinaryFileManager = new BinaryFileManager(this.docIdsBlockPath, MODE.READ, new DeltaCompressor());
            this.freqBinaryFileManager = new BinaryFileManager(this.freqsBlockPath, MODE.READ,  new UnaryCompressor());
        }else{
            this.docIdBinaryFileManager = new BinaryFileManager(this.docIdsBlockPath, MODE.READ);
            this.freqBinaryFileManager = new BinaryFileManager(this.freqsBlockPath, MODE.READ);
        }
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

    private long getMaximumSize(int postingLength){
        if(! useVariableBlockSize){
            return SplittedInvertedIndexBlockManager.skipBlockMaxLength;
        }
        long tmp = Math.round( Math.sqrt(postingLength) );
        if(tmp < SplittedInvertedIndexBlockManager.skipBlockMaxLength){
            return SplittedInvertedIndexBlockManager.skipBlockMaxLength;
        }
        return tmp;
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
        long freqOffset = this.freqBinaryFileManager.getCurrentPosition();
        // here we have to split each posting of the posting array in two integers.
        // we must obtain two arrays of integers, each of them has to be stored on the dedicated file by the binaryfilemanager
        ArrayList<Integer> docIds = new ArrayList<Integer>();
        ArrayList<Integer> freqs = new ArrayList<Integer>();

        final long BLOCK_SIZE = this.getMaximumSize(r.size());

        for (Posting posting : r) {
            docIds.add( posting.getDocid());
            freqs.add(posting.getFreq());
            counter+=1;
            if(counter % BLOCK_SIZE == 0){
                // here we write a whole skip block on file and store its informations
                int docIdWrittenBytes = this.docIdBinaryFileManager.writeIntArray(docIds.subList(startingIndex, docIds.size()).stream().mapToInt(Integer::intValue).toArray());
                int freqWrittenBytes  = this.freqBinaryFileManager.writeIntArray(freqs.subList(startingIndex, docIds.size()).stream().mapToInt(Integer::intValue).toArray());

                SkipBlock sb = new SkipBlock(docIdOffset, freqOffset, docIds.get(counter - 1), counter - startingIndex, docIdWrittenBytes, freqWrittenBytes);
                ret.add(sb);
                
                startingIndex += BLOCK_SIZE;
                docIdOffset += docIdWrittenBytes;
                freqOffset  += freqWrittenBytes;
            }
        }

        // Manage the case in which the posting list has a size multiple of BLOCK_SIZE
        if ( docIds.size() % BLOCK_SIZE == 0 || freqs.size() % BLOCK_SIZE == 0){
            return ret;
        }

        int docIdWrittenBytes = this.docIdBinaryFileManager.writeIntArray(docIds.subList(startingIndex, docIds.size()).stream().mapToInt(Integer::intValue).toArray());
        int freqWrittenBytes  = this.freqBinaryFileManager.writeIntArray(freqs.subList(startingIndex, docIds.size()).stream().mapToInt(Integer::intValue).toArray());

        SkipBlock sb = new SkipBlock(docIdOffset, freqOffset, docIds.get(counter - 1), counter - startingIndex, docIdWrittenBytes, freqWrittenBytes);
        ret.add(sb);
        
        //startingIndex += BLOCK_SIZE;
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
            docIds = this.docIdBinaryFileManager.readIntArray(sb.getDocIdByteSize(), sb.getDocIdFileOffset(), sb.getHowManyPostings());
            freqs = this.freqBinaryFileManager.readIntArray(sb.getFreqByteSize(), sb.getFreqFileOffset(), sb.getHowManyPostings());
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
