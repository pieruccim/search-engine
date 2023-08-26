package common.manager.block;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import common.bean.DocumentIndexFileRecord;
import common.manager.file.FileManager;
import common.manager.file.FileManager.MODE;
import config.ConfigLoader;
import jdk.jshell.spi.ExecutionControl;

public class DocumentIndexBlockManager extends BinaryBlockManager<DocumentIndexFileRecord>{

    protected static String blockDirectory = ConfigLoader.getProperty("blocks.documentIndex.path");
    protected static String mergedBlockFilePath = ConfigLoader.getProperty("blocks.merged.documentIndex.path");

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

    private static int[] temp = new int[3];
    @Override
    public DocumentIndexFileRecord readRow(){


        try {
            int ret = this.binaryFileManager.readUncompressedIntArray(temp);
            //docId = binaryFileManager.readInt();
            //docNo = binaryFileManager.readInt();
            //len = binaryFileManager.readInt();
            if(ret != 3){
                return null;
            }
        } catch (EOFException e){
            return null;
        } catch (Exception e){
            return null;
        } 

        return new DocumentIndexFileRecord(temp[0], temp[1], temp[2]);
    }

    private static int[] intsBuffer = new int[3*1000];
    /**
     * appends to the given list as many DocumentIndexFileRecord objects as possible
     * - the maximum amount of records is conditioned by the buffers sizes
     * @param resultList
     * @return how many DocumentIndexFileRecord were effectively added to the list
     */
    public int readRows(ArrayList<DocumentIndexFileRecord> resultList){

        int howManyRecords = -1;

        try {
            int ret = this.binaryFileManager.readUncompressedIntArray(intsBuffer);

            howManyRecords = ret / 3;
            
            for (int i = 0; i < howManyRecords; i++) {
                resultList.add(new DocumentIndexFileRecord(intsBuffer[0 + i*3], intsBuffer[1 + i*3], intsBuffer[2 + i*3]));
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return howManyRecords;

    }

    /**
     * stores in the given array starting from the given offset as many Document lenghts information as possible
     * - the maximum amount of records is conditioned by the buffers sizes
     * @param documentIndexLengthsInformation
     * @param offset
     * @return how many Document lenghts information were effectively added to the array
     */
    public int readDocumentLenghtsList(int [] documentIndexLengthsInformation, int startingOffset){

        int howManyRecords = -1;

        try {
            int ret = this.binaryFileManager.readUncompressedIntArray(intsBuffer);

            howManyRecords = ret / 3;

            howManyRecords = Math.min(howManyRecords, (documentIndexLengthsInformation.length - startingOffset));
            
            for (int i = 0; i < howManyRecords; i++) {
                documentIndexLengthsInformation[startingOffset + i] = intsBuffer[2 + i*3];
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return howManyRecords;

    }

    public static DocumentIndexBlockManager getMergedFileManager(MODE mode) throws IOException{
        return new DocumentIndexBlockManager(mergedBlockFilePath.replace(".binary", ""), mode);
    }

    public static DocumentIndexBlockManager getMergedFileManager() throws IOException{
        return DocumentIndexBlockManager.getMergedFileManager(MODE.READ);
    }

}
