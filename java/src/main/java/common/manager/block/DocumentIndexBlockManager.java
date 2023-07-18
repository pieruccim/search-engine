package common.manager.block;

import java.io.IOException;

import common.bean.DocumentIndexFileRecord;
import common.manager.file.FileManager;

public class DocumentIndexBlockManager extends BinaryBlockManager<DocumentIndexFileRecord>{

    protected static String blockDirectory = System.getProperty("user.dir") + "/src/main/java/data/output/documentIndexBlocks/";

    public DocumentIndexBlockManager(int blockNo, FileManager.MODE mode) throws IOException {
        super(blockNo, blockDirectory, mode);
    }

    public DocumentIndexBlockManager(String blockName, FileManager.MODE mode) throws IOException {
        super(blockName, blockDirectory, mode);
    }

    @Override
    public void writeRow(DocumentIndexFileRecord r) throws Exception{

        this.binaryFileManager.writeInt(r.getDocId());
        this.binaryFileManager.writeInt(r.getDocNo());
        this.binaryFileManager.writeInt(r.getLen());

    }

    @Override
    public DocumentIndexFileRecord readRow(){

        int docId = 0;
        int docNo = 0;
        int len = 0;

        try {
            docId = binaryFileManager.readInt();
            docNo = binaryFileManager.readInt();
            len = binaryFileManager.readInt();
        } catch (Exception e){
            return null;
        }

        return new DocumentIndexFileRecord(docId, docNo, len);
    }
    
}
