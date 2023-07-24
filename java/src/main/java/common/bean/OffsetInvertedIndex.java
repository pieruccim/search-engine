package common.bean;

import java.util.ArrayList;

public interface OffsetInvertedIndex {
    /**
     * 
     * @return true if docID and frequencies are stored on two separate files, else returns false
     */
    public boolean isInvertedIndexSplit();
    /**
     * 
     * @return the amount of bytes from the beginning of the file where to start to read from
     */
    public long getBytesOffsetDocId();
    /**
     * if isInvertedIndexSplit() == true, returns the same as getBytesOffsetDocId(), 
     * else it returns the offset for the frequency inverted index file
     * @return the amount of bytes from the beginning of the file where to start to read from
     */
    public long getBytesOffsetFreq();
    /**
     * 
     * @return
     */
    public String getStringFileRecord();
    /**
     * moves forward the offset of the specified amount of bytes
     * the length of the ArrayList must be adequate to the Offset implementation
     * @param writtenBytes
     */
    public void forward(ArrayList<WrittenBytes> writtenBytes);
}
