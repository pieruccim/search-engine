package common.manager.block;

import java.io.IOException;

import common.bean.DocumentIndexFileRecord;

public class DocumentIndexBlockManager extends BinaryBlockManager<DocumentIndexFileRecord>{

    protected static String blockDirectory = "/data/output/documentIndexBlocks/";

    public DocumentIndexBlockManager(int blockNo) throws IOException {
        super(blockNo, blockDirectory);
    }

    @Override
    public void writeRow(DocumentIndexFileRecord r) {
        
        this.binaryFileManager.writeInt(r.getDocId());
        this.binaryFileManager.writeInt(r.getDocNo());
        this.binaryFileManager.writeInt(r.getLen());
        
    }
    
}
