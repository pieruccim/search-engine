package common.manager.file;

import java.io.*;

public class BinaryFileManager extends FileManager {

    protected DataInputStream dataInputStream;
    protected DataOutputStream dataOutputStream;

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
                this.dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filePath)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (mode == MODE.READ) {
            try {
                this.randomAccessFileInput = new RandomAccessFile(filePath, "r");
                this.dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(randomAccessFileInput.getFD())));
            } catch (IOException e) {
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
            return this.dataInputStream.readInt();
        }catch(EOFException e){
            throw e; // in case of EOFException, it is thrown directly
        }catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading integer from the binary file");
        }
    }

    /**
     *
     * @param offset represents the number of integers from the beginning of the file
     * @return the read integer
     * @throws Exception
     */
    @Override
    public int readInt(long offset) throws Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readInt");
        }

        try {
            offset = offset * (Integer.SIZE / 8); // TODO: check

            this.seek(offset);

            int value = dataInputStream.readInt(); // Read the integer

            return value;
        } catch (EOFException e) {
            throw new Exception("End of file reached while reading integer");
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("Error reading integer from the binary file");
        }
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
     * implements the seek method in reading mode
     * @param byteOffset offset from the starting of the file in bytes
     * @throws IOException
     */
    protected void seek(long byteOffset) throws IOException, Exception{
        if(this.mode != MODE.READ){
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform seek");
        }
        //this.dataInputStream.close();
        this.randomAccessFileInput.seek(byteOffset);
        //long test = this.randomAccessFileInput.getFilePointer();
        //this.dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(randomAccessFileInput.getFD())));
            
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
                this.dataInputStream.close();
                this.randomAccessFileInput.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean checkDebug(String message){
        if(this.randomAccessFileInput == null){
                System.out.println(message+":\trandomAccessFileInput == null");
                return false;
        }
        return true;

    }
}