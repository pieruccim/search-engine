package common.manager.block;

import java.io.IOException;

import common.bean.VocabularyFileRecord;


public class VocabularyBlockManager extends TextualBlockManager<VocabularyFileRecord>{

    protected static String blockDirectory = "/data/output/vocabularyBlocks/";

    public VocabularyBlockManager(int blockNo) throws IOException {
        super(blockNo, blockDirectory);
    }

    @Override
    public void writeRow(VocabularyFileRecord r) {
        this.textualFileManager.writeLine(r.getTerm() + " " + r.getCf() + " " + r.getDf() + " " + r.getOffset());
    }
    
}
