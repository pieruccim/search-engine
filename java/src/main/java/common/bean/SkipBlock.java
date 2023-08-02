package common.bean;

public class SkipBlock {
    protected long docIdFileOffset;
    protected long freqFileOffset;
    protected int maxDocId;
    protected int howManyPostings;

    public static final int SKIP_BLOCK_ENTRY_SIZE = 2 * 4 + 2 * 8;

    public SkipBlock(long docIdFileOffset, long freqFileOffset, int maxDocId, int howManyPostings) {
        this.docIdFileOffset = docIdFileOffset;
        this.freqFileOffset = freqFileOffset;
        this.maxDocId = maxDocId;
        this.howManyPostings = howManyPostings;
    }

    public long getDocIdFileOffset() {
        return docIdFileOffset;
    }

    public long getFreqFileOffset() {
        return freqFileOffset;
    }

    public int getMaxDocId() {
        return maxDocId;
    }

    public int getHowManyPostings() {
        return howManyPostings;
    }

    public static int getSkipBlockEntrySize() {
        return SKIP_BLOCK_ENTRY_SIZE;
    }
}
