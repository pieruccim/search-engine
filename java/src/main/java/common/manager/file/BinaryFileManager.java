package common.manager.file;

import java.io.*;

public class BinaryFileManager extends FileManager {

    protected DataOutputStream dataOutputStream;
    protected BufferedOutputStream bufferedOutputStream;

    protected RandomAccessFile randomAccessFileInput;

    public BinaryFileManager(String filePath) {
        super(filePath);
    }

    public BinaryFileManager(String filePath, MODE mode) {
        super(filePath, mode);
    }

    @Override
    protected void initialSetup(String filePath, MODE mode) {
        this.mode = mode;
        this.filePath = filePath;
        if (mode == MODE.WRITE) {
            try {
                this.bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(filePath));
                this.dataOutputStream = new DataOutputStream(bufferedOutputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (mode == MODE.READ) {
            try {
                this.randomAccessFileInput = new RandomAccessFile(filePath, "r");
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
            return this.randomAccessFileInput.readInt();
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
            int value = randomAccessFileInput.readInt();

            return value;
        } catch (EOFException e) {
            throw new Exception("End of file reached while reading integer");
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading integer from the binary file");
        }
    }

    public byte[] readByteArray() throws Exception{
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readByteArray");
        }
        // TODO
        return new byte[0];
    }


    @Override
    public void writeInt(int in) throws Exception {
        if(this.mode != MODE.WRITE){
            throw new Exception("Binary file manager not in MODE.WRITE\tCannot perform writeInt");
        }
        try {
            this.dataOutputStream.writeInt(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param byteArray
     * @throws Exception
     */
    public void writeByteArray(byte[] byteArray) throws Exception{
        if(this.mode != MODE.WRITE){
            throw new Exception("Binary file manager not in MODE.WRITE\tCannot perform writeByteArray");
        }
        try {
            this.bufferedOutputStream.write(byteArray);
            System.out.println("Successfully wrote the Byte Array to file");
        } catch (IOException e) {
            System.err.println("Error occurred while writing Byte Array to file");
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
        this.randomAccessFileInput.seek(byteOffset);

    }

    public long getCurrentPosition() throws IOException{
        //if(this.mode != MODE.READ){
        //    throw new Exception("Binary file manager not in MODE.READ\tCannot perform seek");
        //}
        return this.randomAccessFileInput.getFilePointer();
    }

    @Override
    public void close() {
        if (this.mode == MODE.WRITE) {
            try {
                dataOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (this.mode == MODE.READ) {
            try {
                this.randomAccessFileInput.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}