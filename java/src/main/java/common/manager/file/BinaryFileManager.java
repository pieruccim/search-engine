package common.manager.file;

import common.manager.file.compression.Compressor;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;

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
        if(this.compressor != null){
            throw new Exception("Cannot invoke readInt on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
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
        if(this.compressor != null){
            throw new Exception("Cannot invoke readInt on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
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

    public float readFloat() throws EOFException, Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readFloat");
        }
        if(this.compressor != null){
            throw new Exception("Cannot invoke readFloat on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
        }
        try {
            return this.randomAccessFile.readFloat();
        }catch(EOFException e){
            throw e; // in case of EOFException, it is thrown directly
        }catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading float from the binary file");
        }
    }

    public float readFloat(long offset) throws Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readFloat");
        }
        if(this.compressor != null){
            throw new Exception("Cannot invoke readFloat on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
        }
        try {
            this.seek(offset);
            return randomAccessFile.readFloat();
        } catch (EOFException e) {
            throw new Exception("End of file reached while reading float");
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading float from the binary file");
        }
    }

    public double readDouble() throws EOFException, Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readDouble");
        }
        if(this.compressor != null){
            throw new Exception("Cannot invoke readDouble on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
        }
        try {
            return this.randomAccessFile.readDouble();
        }catch(EOFException e){
            throw e; // in case of EOFException, it is thrown directly
        }catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading double from the binary file");
        }
    }

    protected static final int    doubleBytes = Double.SIZE / Byte.SIZE;
    protected static byte[] buffer = new byte[doubleBytes * 2000];
    /**
     * loads as many doubles as possible and stores them in outputArrayList
     * @param outputArrayList
     * @return the number of double that were read
     * @throws EOFException
     * @throws Exception
     */
    public int readDoubleArray(ArrayList<Double> outputArrayList) throws EOFException, Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readDouble");
        }
        if(this.compressor != null){
            throw new Exception("Cannot invoke readDouble on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
        }
        try {
            int numBytes = this.randomAccessFile.read(buffer);
            int numDoubles = numBytes / doubleBytes;
            for(int i=0; i < numDoubles; i++){
                //outputBuffer[offset + i] = ByteBuffer.wrap(buffer, i*doubleBytes, doubleBytes).getDouble();
                outputArrayList.add( ByteBuffer.wrap(buffer, i*doubleBytes, doubleBytes).getDouble() );
                
            }
            return numDoubles;

        }catch(EOFException e){
            throw e; // in case of EOFException, it is thrown directly
        }catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading double from the binary file");
        }
    }

    public double readDouble(long offset) throws Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readDouble");
        }
        if(this.compressor != null){
            throw new Exception("Cannot invoke readDouble on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
        }
        try {
            this.seek(offset);
            return randomAccessFile.readDouble();
        } catch (EOFException e) {
            throw new Exception("End of file reached while reading double");
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading double from the binary file");
        }
    }

    private byte[] readByteArray(int byteSize, long fileOffset) throws Exception{
        byte[] compressedData = new byte[byteSize];

        /*try(FileChannel fChan = (FileChannel) Files.newByteChannel(Paths.get(this.filePath), StandardOpenOption.READ)) {

            // Instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_ONLY,fileOffset, byteSize);

            if (buffer == null) {
                return null;
            }

            // Read bytes from file
            buffer.get(compressedData, 0, byteSize);
        }*/

        this.randomAccessFile.seek(fileOffset);
        this.randomAccessFile.read(compressedData, 0, byteSize);

        return compressedData;
    }

    public int[] readIntArray(int byteSize, long fileOffset, int howManyInt) throws Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readIntArray");
        }

        // Check if the compressor is used by this BinaryFileManager
        // If the compressor is not declared ...
        if (compressor == null){
            //byte[] data = new byte[byteSize];
            //randomAccessFile.seek(fileOffset);
            //randomAccessFile.read(data);
            //int [] ret = new int[howManyInt];
            //for (int i = 0; i < ret.length; i++) {
            //    ret[i] =    ((data[(i << 2)    ] & 0xFF) << 24) | 
            //                ((data[(i << 2) + 1] & 0xFF) << 16) | 
            //                ((data[(i << 2) + 2] & 0xFF) << 8 ) | 
            //                ((data[(i << 2) + 3] & 0xFF) << 0 );
            //}
            //return ret;
            this.randomAccessFile.seek(fileOffset);
            int [] ret = new int[howManyInt];
            for (int i = 0; i < ret.length; i++) {
                ret[i] = this.randomAccessFile.readInt();
            }
            return ret;
        }
        // If the compressor is declared the byte array is decompressed with the preferred method
        else{
            byte[] compressedData = readByteArray(byteSize, fileOffset);
            return compressor.decompressIntArray(compressedData, howManyInt);
        }
    }

    public long readLong() throws Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readLong");
        }
        if(this.compressor != null){
            throw new Exception("Cannot invoke readLong on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
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
        if(this.compressor != null){
            throw new Exception("Cannot invoke writeInt on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
        }
        try {
            //unused DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(randomAccessFile.getFD())));
            this.randomAccessFile.writeInt(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeFloat(float in) throws Exception{
        if(this.mode != MODE.WRITE){
            throw new Exception("Binary file manager not in MODE.WRITE\tCannot perform writeFloat");
        }
        if(this.compressor != null){
            throw new Exception("Cannot invoke writeFloat on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
        }
        try {
            this.randomAccessFile.writeFloat(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeDouble(double in) throws Exception{
        if(this.mode != MODE.WRITE){
            throw new Exception("Binary file manager not in MODE.WRITE\tCannot perform writeDouble");
        }
        if(this.compressor != null){
            throw new Exception("Cannot invoke writeDouble on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
        }
        try {
            this.randomAccessFile.writeDouble(in);
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
            //System.out.println("Successfully wrote the Byte Array to file");
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
            //System.out.println("Number of integers to write: " + intArray.length);
            byte[] compressedArray = compressor.compressIntArray(intArray);
            writeByteArray(compressedArray);
            return compressedArray.length;
        }
    }

    public void writeLong(long inLong) throws Exception {
        if(this.mode != MODE.WRITE){
            throw new Exception("Binary file manager not in MODE.WRITE\tCannot perform writeLong");
        }
        if(this.compressor != null){
            throw new Exception("Cannot invoke writeLong on a file opened with a compressor! Current compressor: "
             + this.compressor.getClass().getName() + " | file: " + this.filePath);
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