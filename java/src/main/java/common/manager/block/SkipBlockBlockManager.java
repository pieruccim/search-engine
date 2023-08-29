package common.manager.block;

import java.io.IOException;

import common.bean.SkipBlock;
import common.manager.file.FileManager;
import config.ConfigLoader;

public class SkipBlockBlockManager extends BinaryBlockManager<SkipBlock>{
    protected static String blockDirectory = ConfigLoader.getProperty("blocks.skipBlock.path");

    public SkipBlockBlockManager(int blockNo, FileManager.MODE mode) throws IOException {
        super(blockNo, blockDirectory, mode);
    }

    public SkipBlockBlockManager(String blockName, FileManager.MODE mode) throws IOException {
        super(blockName, blockDirectory, mode);
    }

    @Override
    public void writeRow(SkipBlock r) throws Exception {
        this.binaryFileManager.writeLong(r.getDocIdFileOffset());
        this.binaryFileManager.writeLong(r.getFreqFileOffset());
        this.binaryFileManager.writeInt(r.getMaxDocId());
        this.binaryFileManager.writeInt(r.getHowManyPostings());
        this.binaryFileManager.writeInt(r.getDocIdByteSize());
        this.binaryFileManager.writeInt(r.getFreqByteSize());
    }

    @Override
    public SkipBlock readRow() throws Exception {
        long docIdOffset = this.binaryFileManager.readLong();
        long freqOffset  = this.binaryFileManager.readLong();
        int maxDocId     = this.binaryFileManager.readInt();
        int howMany      = this.binaryFileManager.readInt();
        int docIdByteSize= this.binaryFileManager.readInt();
        int freqByteSize = this.binaryFileManager.readInt();
        return new SkipBlock(docIdOffset, freqOffset, maxDocId, howMany, docIdByteSize, freqByteSize);
    }

    /**
     * reads a SkipBlock at the given offset (in bytes), the FP position is changed
     * @param offset
     * @return
     * @throws Exception
     */
    public SkipBlock readRowAt(long offset) throws Exception {
        //long prev = this.binaryFileManager.getCurrentPosition();
        this.seek(offset);
        SkipBlock sb = this.readRow();
        //this.seek(prev);
        return sb;
    }

    public void seek(long offset) throws Exception{
        try {
            this.binaryFileManager.seek(offset);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
