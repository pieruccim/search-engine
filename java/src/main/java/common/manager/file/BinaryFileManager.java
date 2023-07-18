package common.manager.file;

import java.io.*;

public class BinaryFileManager extends FileManager {

    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;

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
                this.dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int readInt() throws Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readInt");
        }

        try {
            return this.dataInputStream.readInt();
        } catch (EOFException e) {
            throw new Exception("End of file reached while reading integer");
        } catch (IOException e) {
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
    public int readInt(int offset) throws Exception {
        if (this.mode != MODE.READ) {
            throw new Exception("Binary file manager not in MODE.READ\tCannot perform readInt");
        }

        try {
            offset = offset * (Integer.SIZE / 8); // TODO: check
            dataInputStream.reset(); // Reset to the beginning of the stream
            dataInputStream.skipBytes(offset); // Skip bytes to reach the specified offset
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
                dataInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}