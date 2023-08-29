package common.manager.block;

import java.io.IOException;

import common.bean.VocabularyFileRecord;
import common.manager.file.FileManager;
import common.manager.file.FileManager.MODE;
import config.ConfigLoader;



public class VocabularyBlockManager extends TextualBlockManager<VocabularyFileRecord>{

    protected static String blockDirectory = ConfigLoader.getProperty("blocks.vocabulary.path");

    protected static String mergedBlockFilePath = ConfigLoader.getProperty("blocks.merged.vocabulary.path");

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
        this.textualFileManager.writeLine(r.getTerm() + " " + r.getCf() + " " + r.getDf() + " " + r.getOffset() + " " + r.getHowManySkipBlocks());
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
        
        return new VocabularyFileRecord(arrayString[0], Integer.parseInt(arrayString[1]), Integer.parseInt(arrayString[2]), Integer.parseInt(arrayString[3]), Integer.parseInt(arrayString[4]));
    }

    public static VocabularyBlockManager getMergedFileManager(MODE mode) throws IOException{
        return new VocabularyBlockManager(mergedBlockFilePath.replace(".txt", ""), mode);
    }

    public static VocabularyBlockManager getMergedFileManager() throws IOException{
        return getMergedFileManager(MODE.READ);
    }

}
