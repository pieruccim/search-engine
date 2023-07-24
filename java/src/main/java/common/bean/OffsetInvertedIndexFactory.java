package common.bean;

import config.ConfigLoader;
import common.manager.block.InvertedIndexBlockManager;
import common.manager.block.VocabularyBlockManager;
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

    public static OffsetInvertedIndex parseObjectFromString(String inpuString){
        if(OffsetInvertedIndexFactory.offsetType == OffsetType.SINGLE_FILE){
            return OffsetIISingleFile.parseFromString(inpuString);
        }else if(OffsetInvertedIndexFactory.offsetType == OffsetType.TWO_FILES){
            return OffsetIITwoFiles.parseFromString(inpuString);
        }else{
            throw new UnsupportedOperationException("Cannot determine the OffsetObject type");
        }
    }
}
