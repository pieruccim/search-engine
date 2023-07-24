package common.bean;

import java.util.ArrayList;

public class OffsetIITwoFiles implements OffsetInvertedIndex{

    private int offsetDocId;
    private int offsetFreq;
    // the delimiter must be different from the delimiters of the other structures 
    private static final String delimiter = "-";

    public OffsetIITwoFiles(int offsetDocId, int offsetFreq){
        this.offsetDocId = offsetDocId;
        this.offsetFreq = offsetFreq;
    }

    @Override
    public boolean isInvertedIndexSplit() {
        return true;
    }

    @Override
    public long getBytesOffsetDocId() {
        return this.offsetDocId;
    }

    @Override
    public long getBytesOffsetFreq() {
        return this.offsetFreq;
    }

    /**
     * the fields are splitted by a '-' character and not a space in order to not conflict with the delimiter of the vocabulary file record
     */
    @Override
    public String getStringFileRecord() {
        return this.offsetDocId + OffsetIITwoFiles.delimiter + this.offsetFreq;
    }

    public static OffsetInvertedIndex parseFromString(String sourceString){
        String[] arrayString = sourceString.split(OffsetIITwoFiles.delimiter);
        return new OffsetIITwoFiles(Integer.parseInt(arrayString[0]), Integer.parseInt(arrayString[1]));
    }

    @Override
    public void forward(ArrayList<WrittenBytes> writtenBytes) {
        // the length of the ArrayList writtenBytes must be equal to 2 in this case
        if(writtenBytes.size() != 2){
            throw new IllegalArgumentException("the length of the ArrayList writtenBytes must be equal to 2; \t" + writtenBytes.size() + " elements were given");
        }
        this.offsetDocId += writtenBytes.get(0).getNumBytes();
        this.offsetFreq  += writtenBytes.get(1).getNumBytes();
    }
}
