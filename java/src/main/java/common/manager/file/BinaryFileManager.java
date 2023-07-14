package common.manager.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class BinaryFileManager extends FileManager{

    private BufferedOutputStream bufferedOutputStream;

    public BinaryFileManager(String filePath) {
        super(filePath);
    }

    public BinaryFileManager(String filePath, MODE mode) {
        super(filePath, mode);
    }

    @Override
    protected void initialSetup(String filePath, MODE mode){
        if(mode == MODE.WRITE){
            try{
                this.bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(filePath));
            }catch (IOException e){
                e.printStackTrace();
            }
        }else{  //  if(this.mode == MODE.READ)
            throw new UnsupportedOperationException("Unimplemented method 'initialSetup' for MODE.READ");
        }

    }

    @Override
    public int readInt() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'readInt'");
    }

    @Override
    public void writeInt(int in) {
        try {
            this.bufferedOutputStream.write(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if(this.mode == MODE.WRITE){
            try{
                bufferedOutputStream.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

}
