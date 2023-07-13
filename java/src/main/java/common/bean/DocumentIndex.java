package common.bean;

import java.util.HashMap;

public class DocumentIndex {

    private class DocumentIndexInformation{

        private int len;
        private int docno;

        public DocumentIndexInformation(int len, int docno){
            this.len = len;
            this.docno = docno;
        }

        public int getLen() {
            return len;
        }

        public void setLen(int len) {
            this.len = len;
        }

        public int getDocno() {
            return docno;
        }

        public void setDocno(int docno) {
            this.docno = docno;
        }

        // useful for debugging
        @Override
        public String toString() {
            return "DocumentIndexInformation{" +
                    "len=" + this.len +
                    ", docno=" + this.docno +
                    '}';
        }
    }

    // the document Index is implemented by an HashMap that links a docid (int) and the corresponding information (len,docno)
    HashMap<Integer, DocumentIndexInformation> documentIndex;

    public DocumentIndex(){
        this.documentIndex = new HashMap<Integer, DocumentIndexInformation>();
    }

    public HashMap<Integer, DocumentIndexInformation> getDocumentIndex() {return documentIndex;}

    public void setDocumentIndex(HashMap<Integer, DocumentIndexInformation> d) {
        this.documentIndex = d;
    }

    public void reset() {this.setDocumentIndex(new HashMap<>());}

    public void addInformation(int len, int docid, int docno){
        DocumentIndexInformation newInformation = new DocumentIndex.DocumentIndexInformation(len, docno);
        this.documentIndex.put(docid, newInformation);
    }

    public String toString(Integer docid) {
        DocumentIndex.DocumentIndexInformation information = documentIndex.get(docid);
        return (information != null) ? "Docid: " + docid + ", " + information.toString() : "Docid not found in vocabulary.";
    }
}
