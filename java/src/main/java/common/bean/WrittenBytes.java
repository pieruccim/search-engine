package common.bean;

public class WrittenBytes {
    protected long writtenBytes;

    public WrittenBytes(long writtenBytes){
        this.writtenBytes = writtenBytes;
    }
    /**
     * 
     * @return the amount of bytes written
     */
    public long getNumBytes(){
        return this.writtenBytes;
    }
}
