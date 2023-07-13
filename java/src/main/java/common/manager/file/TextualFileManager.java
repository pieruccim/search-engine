package common.manager.file;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class TextualFileManager extends FileManager{

    private static String charset = "UTF-16";

    private BufferedReader reader;



    public TextualFileManager(String filePath){
        super(filePath);
    }

    public TextualFileManager(String filePath, MODE mode) {
        super(filePath, mode);
    }

    @Override
    protected void initialSetup(String filePath, MODE mode){
        this.mode = mode;
        this.filePath = filePath;
        // here we have to open the file in the specified mode
        if(this.mode == MODE.READ){
            try{
                this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), TextualFileManager.charset));
            }catch (IOException e){
                e.printStackTrace();
            }
        }else{  //  if(this.mode == MODE.WRITE)
            throw new UnsupportedOperationException("Unimplemented method 'initialSetup' for MODE.WRITE");
        }
    }

    /**
     * @return int the next parsed integer, or -1 in case of empty file or reader not ready
     * this works since all the expected integer values are positive
     */
    @Override
    int readInt() {
        try{
            if(reader.ready()){
                return Integer.parseInt(Character.toString((char) reader.read()));
            }
            else return -1;
        }catch (Exception e) {
            //e.printStackTrace();
            return -1;
        }
    }

    /**
     * 
     * @return 
     * - String the string of characters until the '\n' is found
     * - null if the reader is not ready or the file is empty or ended
     * 
     * //@ exception e if the scanner is closed or the file is empty, throws exception
     */
    public String readLine(){
        try {
            if(reader.ready())
                return reader.readLine();
            else
                return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    void writeInt(int in) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'writeInt'");
    }

    @Override
    void close() {
        if(this.mode == MODE.READ){
            try {
                this.reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
