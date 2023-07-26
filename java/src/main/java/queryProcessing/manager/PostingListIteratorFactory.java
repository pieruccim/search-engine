package queryProcessing.manager;

import common.bean.VocabularyFileRecord;
import common.manager.block.VocabularyBlockManager.OffsetType;
import config.ConfigLoader;

public class PostingListIteratorFactory {

    protected static OffsetType offsetType = OffsetType.valueOf(ConfigLoader.getProperty("blocks.invertedindex.type"));

    public static PostingListIterator openIterator(VocabularyFileRecord vocabularyFileRecord){

        PostingListIterator postingListIterator = null;
        switch (PostingListIteratorFactory.offsetType) {
            case SINGLE_FILE:
                postingListIterator = new PostingListIteratorSingleFile();
                break;
            //case TWO_FILES:
                //return new PostingListIteratorTwoFile();
            default:
                throw new UnsupportedOperationException("Unimplemented OffsetType handling for " + PostingListIteratorFactory.offsetType);
        }

        postingListIterator.openList(vocabularyFileRecord);
        return postingListIterator;
    }
}
