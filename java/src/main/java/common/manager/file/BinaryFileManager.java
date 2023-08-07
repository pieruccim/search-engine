package common.manager.file;

import common.manager.file.compression.Compressor;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class BinaryFileManager extends FileManager {

    protected RandomAccessFile randomAccessFile;

    protected Compressor compressor;

    public BinaryFileManager(String filePath) {
        super(filePath);
        this.compressor = null;
    }

    public BinaryFileManager(String filePath, MODE mode) {
        super(filePath, mode);
        this.compressor = null;
    }

    public BinaryFileManager(String filePath, MODE mode, Compressor compressor){
        super(filePath, mode);
        this.compressor = compressor;
    }

    @Override
    protected void initialSetup(String filePath, MODE mode) {
        this.mode = mode;
        this.filePath = filePath;
        if (mode == MODE.WRITE) {
            try {
                this.randomAccessFile = new RandomAccessFile(filePath, "rw");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (mode == MODE.READ) {
            try {
                this.randomAccessFile = new RandomAccessFile(filePath, "r");
            } catch (IOException e) {
                System.out.println("Received filePath: " + filePath);
                e.printStackTrace();
            }
        }
    }

    /**
     * @throws EOFException when the input file is empty
     * @throws Exception when the FileManager is not in READ mode or in case of IOException
     */
    @Override
    public int readInt() throws EOFException, Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readInt");
        }
        try {
            return this.randomAccessFile.readInt();
        }catch(EOFException e){
            throw e; // in case of EOFException, it is thrown directly
        }catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading integer from the binary file");
        }
    }

    /**
     *
     * @param offset represents the number of bytes from the beginning of the file
     * @return the read integer
     * @throws Exception
     */
    @Override
    public int readInt(long offset) throws Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readInt");
        }

        try {
            this.seek(offset);
            int value = randomAccessFile.readInt();

            return value;
        } catch (EOFException e) {
            throw new Exception("End of file reached while reading integer");
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading integer from the binary file");
        }
    }

    private byte[] readByteArray(int byteSize) throws Exception{
        byte[] compressedData = new byte[byteSize];

        try(FileChannel fChan = (FileChannel) Files.newByteChannel(Paths.get(this.filePath), StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            // Instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE,0, byteSize);

            if (buffer == null) {
                return null;
            }

            // Read bytes from file
            buffer.get(compressedData, 0, byteSize);
        }

        return compressedData;
    }

    public int[] readIntArray(int byteSize, int howManyInt) throws Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readIntArray");
        }

        // Check if the compressor is used by this BinaryFileManager
        // If the compressor is not declared ...
        if (compressor == null){
            //TODO: handle the case in which compressor is not present
            return new int[0];
        }
        // If the compressor is declared the byte array is decompressed with the preferred method
        else{
            byte[] compressedData = readByteArray(byteSize);
            return compressor.decompressIntArray(compressedData, howManyInt);
        }
    }

    public long readLong() throws Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readLong");
        }
        try {
            return this.randomAccessFile.readLong();
        }catch(EOFException e){
            throw e; // in case of EOFException, it is thrown directly
        }catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading long from the binary file");
        }
    }


    @Override
    public void writeInt(int in) throws Exception {
        if(this.mode != MODE.WRITE){
            throw new Exception("Binary file manager not in MODE.WRITE\tCannot perform writeInt");
        }
        try {
            //unused DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(randomAccessFile.getFD())));
            this.randomAccessFile.writeInt(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param byteArray
     * @throws Exception
     */
    private void writeByteArray(byte[] byteArray){
        try {
            randomAccessFile.write(byteArray);
            //BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(randomAccessFile.getFD()));
            //bufferedOutputStream.write(byteArray);
            System.out.println("Successfully wrote the Byte Array to file");
        } catch (IOException e) {
            System.err.println("Error occurred while writing Byte Array to file");
        }
    }

    public int writeIntArray(int[] intArray) throws Exception{
        if(this.mode != MODE.WRITE){
            throw new Exception("Binary file manager not in MODE.WRITE\tCannot perform writeByteArray");
        }

        // Check if the compressor is used by this BinaryFileManager
        // If the compressor is not declared call writeInt() on each element of the integer array
        if (compressor == null){
            for(int value: intArray){
                writeInt(value);
            }
            return intArray.length * 4;
        }
        // If the compressor is declared the integer array is compressed with the preferred method
        else{
            byte[] compressedArray = compressor.compressIntArray(intArray);
            writeByteArray(compressedArray);
            return compressedArray.length;
        }
    }

    public void writeLong(long inLong) throws Exception {
        if(this.mode != MODE.WRITE){
            throw new Exception("Binary file manager not in MODE.WRITE\tCannot perform writeLong");
        }
        try {
            this.randomAccessFile.writeLong(inLong);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * implements the seek method in reading mode
     * @param byteOffset offset from the starting of the file in bytes
     * @throws IOException
     */
    public void seek(long byteOffset) throws IOException, Exception{
        if(this.mode != MODE.READ){
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform seek");
        }
        this.randomAccessFile.seek(byteOffset);

    }

    public long getCurrentPosition() throws IOException{
        if(this.mode == MODE.READ){
            return this.randomAccessFile.getFilePointer();
        } else if (this.mode == MODE.WRITE){
            return this.randomAccessFile.getFilePointer();
        }
        // Never returned, as the mode is always READ or WRITE
        return 0;
    }

    @Override
    public void close() {
        if (this.mode == MODE.WRITE) {
            try {
                this.randomAccessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (this.mode == MODE.READ) {
            try {
                this.randomAccessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}