package common.bean;

import config.ConfigLoader;
import common.manager.block.InvertedIndexBlockManager;
import common.manager.block.VocabularyBlockManager.OffsetType;

public class OffsetInvertedIndexFactory {

    protected static OffsetType offsetType = OffsetType.valueOf(ConfigLoader.getProperty("blocks.invertedindex.type"));

    public static OffsetInvertedIndex createZeroOffsetObject() throws Exception{
        switch (OffsetInvertedIndexFactory.offsetType) {
            case SINGLE_FILE:
                return new OffsetIISingleFile(0);
            case TWO_FILES:
                return new OffsetIITwoFiles(0, 0);
            default:
                throw new UnsupportedOperationException("Unimplemented OffsetType handling for " + OffsetInvertedIndexFactory.offsetType);
        }
    }
}
