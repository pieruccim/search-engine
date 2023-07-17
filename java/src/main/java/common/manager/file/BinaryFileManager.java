package common.manager.file;

import java.io.*;

public class BinaryFileManager extends FileManager {

    private BufferedInputStream bufferedInputStream;
    private BufferedOutputStream bufferedOutputStream;

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
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (mode == MODE.READ) {
            try {
                this.bufferedInputStream = new BufferedInputStream(new FileInputStream(filePath));
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
            byte[] buffer = new byte[4];
            int bytesRead = bufferedInputStream.read(buffer);
            if (bytesRead == -1) {
                throw new Exception("End of file reached while reading integer");
            }
            int value = ((buffer[0] & 0xFF) << 24) |
                    ((buffer[1] & 0xFF) << 16) |
                    ((buffer[2] & 0xFF) << 8) |
                    (buffer[3] & 0xFF);
            return value;
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
            this.bufferedOutputStream.write(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (this.mode == MODE.WRITE) {
            try {
                bufferedOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (this.mode == MODE.READ) {
            try {
                bufferedInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}