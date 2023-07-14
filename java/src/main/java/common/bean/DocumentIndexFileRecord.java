package common.bean;

public class DocumentIndexFileRecord {
    protected int docId;
    protected int docNo;
    protected int len;

    public DocumentIndexFileRecord(int docId, int docNo, int len) {
        this.docId = docId;
        this.docNo = docNo;
        this.len = len;
    }
    
    public int getDocId() {
        return docId;
    }
    public int getDocNo() {
        return docNo;
    }
    public int getLen() {
        return len;
    }
    
}
