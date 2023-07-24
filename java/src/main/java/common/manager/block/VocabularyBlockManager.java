package common.manager.block;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;

import common.bean.OffsetIISingleFile;
import common.bean.OffsetIITwoFiles;
import common.bean.OffsetInvertedIndex;
import common.bean.OffsetInvertedIndexFactory;
import common.bean.VocabularyFileRecord;
import common.manager.file.FileManager;
import config.ConfigLoader;
import jdk.jshell.spi.ExecutionControl;


public class VocabularyBlockManager extends TextualBlockManager<VocabularyFileRecord>{

    protected static String blockDirectory = ConfigLoader.getProperty("blocks.vocabulary.path");

    public enum OffsetType{
        SINGLE_FILE,
        TWO_FILES
    };

    protected static OffsetType offsetType = OffsetType.valueOf(ConfigLoader.getProperty("blocks.invertedindex.type"));

    public VocabularyBlockManager(int blockNo, FileManager.MODE mode) throws IOException {
        super(blockNo, blockDirectory, mode);
    }

    public VocabularyBlockManager(String blockName, FileManager.MODE mode) throws IOException {
        super(blockName, blockDirectory, mode);
    }

    @Override
    public void writeRow(VocabularyFileRecord r) {
        this.textualFileManager.writeLine(r.getTerm() + " " + r.getCf() + " " + r.getDf() + " " + (r.getOffset()).getStringFileRecord());
    }

    /**
     *
     * @return vocabularyFileRecord or null if the reader is not ready or the file is empty or ended
     * @throws Exception
     */
    @Override
    public VocabularyFileRecord readRow() throws Exception {
        String line = textualFileManager.readLine();
        if(line == null){
            return null;
        }
        String[] arrayString = line.split(" ");

        // TODO: assert arrayString.length() == 4
        
        OffsetInvertedIndex offsetInvertedIndex = OffsetInvertedIndexFactory.parseObjectFromString(arrayString[3]);

        return new VocabularyFileRecord(arrayString[0], Integer.parseInt(arrayString[1]), Integer.parseInt(arrayString[2]), offsetInvertedIndex);
    }

}
