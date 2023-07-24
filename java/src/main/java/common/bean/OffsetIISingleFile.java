package common.bean;

import java.util.ArrayList;

public class OffsetIISingleFile implements OffsetInvertedIndex {

    private long bytesOffset;

    public OffsetIISingleFile(long bytesOffset){
        this.bytesOffset = bytesOffset;
    }

    @Override
    public boolean isInvertedIndexSplit() {
        return false;
    }

    @Override
    public long getBytesOffsetDocId() {
        return this.bytesOffset;
    }

    @Override
    public long getBytesOffsetFreq() {
        return this.bytesOffset;
    }

    @Override
    public String getStringFileRecord() {
        return String.valueOf(this.bytesOffset);
    }

    public static OffsetInvertedIndex parseFromString(String sourceString){
        return new OffsetIISingleFile(Integer.parseInt(sourceString));
    }

    @Override
    public void forward(ArrayList<WrittenBytes> writtenBytes) {
        // the length of the ArrayList writtenBytes must be equal to 1 in this case
        if(writtenBytes.size() != 1){
            throw new IllegalArgumentException("the length of the ArrayList writtenBytes must be equal to 1; \t" + writtenBytes.size() + " was given");
        }
        if(writtenBytes.get(0).getNumBytes() <= 0){
            throw new IllegalArgumentException("numbytes written is <= 0! received value: " + writtenBytes.get(0).getNumBytes());
        }
        this.bytesOffset += writtenBytes.get(0).getNumBytes();
    }
    
}
