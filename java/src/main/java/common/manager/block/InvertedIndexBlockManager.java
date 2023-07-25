package common.manager.block;

import common.bean.DocumentIndexFileRecord;
import common.bean.OffsetInvertedIndex;
import common.bean.Posting;
import common.bean.WrittenBytes;
import common.manager.block.VocabularyBlockManager.OffsetType;
import common.manager.file.FileManager;
import config.ConfigLoader;
import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;

public class InvertedIndexBlockManager extends BinaryBlockManager<ArrayList<Posting>> {

    protected static String blockDirectory = ConfigLoader.getProperty("blocks.invertedIndex.path");

    protected static OffsetType offsetType = OffsetType.valueOf(ConfigLoader.getProperty("blocks.invertedindex.type"));

    public InvertedIndexBlockManager(int blockNo, FileManager.MODE mode) throws IOException{
        super(blockNo, blockDirectory, mode);
    }

    public InvertedIndexBlockManager(String blockName, FileManager.MODE mode) throws IOException{
        super(blockName, blockDirectory, mode);
    }

    /**
     * @param r<Posting> r a complete posting list for a certain term
     */
    @Override
    public void writeRow(ArrayList<Posting> r) throws Exception{
        if(InvertedIndexBlockManager.offsetType == OffsetType.SINGLE_FILE){
            for (Posting posting : r) {
                this.binaryFileManager.writeInt(posting.getDocid());
                this.binaryFileManager.writeInt(posting.getFreq());
            }
            // when we are here, in the binary output file we have r.length * 2 integers
        }else{
            throw new Exception("Unimplemented OffsetType handling for " + InvertedIndexBlockManager.offsetType);
        }
    }

    /**
     * @param r<Posting> r a complete posting list for a certain term
     * @return ArrayList<WrittenBytes> the length must be equal to 1 in case of inverted index on single file, equal to 2 in case of TWO_FILES
     */
    public ArrayList<WrittenBytes> writeRowReturnWriteInfos(ArrayList<Posting> r) throws Exception{
        ArrayList<WrittenBytes> ret = new ArrayList<WrittenBytes>();
        if(InvertedIndexBlockManager.offsetType == OffsetType.SINGLE_FILE){
            long numBytesWritten = 0;
            for (Posting posting : r) {
                this.binaryFileManager.writeInt(posting.getDocid());
                this.binaryFileManager.writeInt(posting.getFreq());
                numBytesWritten += ( Integer.SIZE / 8) * 2;
            }
            // when we are here, in the binary output file we have r.length * 2 integers
            ret.add(new WrittenBytes(numBytesWritten));
            return ret;
        }else{
            // here the ArrayList must be made of two elements
            throw new Exception("Unimplemented OffsetType handling for " + InvertedIndexBlockManager.offsetType);
        }
    }

    @Override
    public ArrayList<Posting> readRow() throws Exception {
        return null;
    }

    /**
     * 
     * @param offset from the beginning of the block file to read from as number of integers (of 4 bytes)
     * @param numPostings how many posting have to be read (each posting is a pair of integers (docID, freq))
     * @return ArrayList<Posting> the List of postings retrieved
     * @throws Exception
     */
    public ArrayList<Posting> readRow(OffsetInvertedIndex offset, int numPostings) throws Exception {

        ArrayList<Posting> postingList = new ArrayList<>();

        this.seek(offset);

        for(int i = 0; i < numPostings; i++) {
            Pair<Integer, Integer> docIdFreq = readCouple();
            if(docIdFreq == null){
                System.out.println("docIdFreq Pair was returned null");
                break;
            }
            Posting posting = new Posting(docIdFreq.getKey(), docIdFreq.getValue());
            postingList.add(posting);
        }

        return postingList;
    }

    public void seek(OffsetInvertedIndex offsetInvertedIndex) throws Exception{
        if(InvertedIndexBlockManager.offsetType == OffsetType.SINGLE_FILE){
            try {
                this.binaryFileManager.seek(offsetInvertedIndex.getBytesOffsetDocId());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            throw new Exception("Unimplemented seek method for offset type " + InvertedIndexBlockManager.offsetType);
        }
    }

    public Pair<Integer, Integer> readCouple() throws Exception{
        int docId = 0;
        int freq = 0;
        if(InvertedIndexBlockManager.offsetType == OffsetType.SINGLE_FILE){
            try {
                docId = binaryFileManager.readInt();
                freq = binaryFileManager.readInt();
            } catch (Exception e){
                e.printStackTrace();
                return null;
            }
        }else{
            throw new Exception("Unimplemented readCouple method for offset type " + InvertedIndexBlockManager.offsetType);
        }

        return new Pair<>(docId,freq);
    }

}
