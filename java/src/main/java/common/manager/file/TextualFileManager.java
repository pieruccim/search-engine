package common.manager.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class TextualFileManager extends FileManager{

    private static String charset = "UTF-16";

    private BufferedReader reader;
    private BufferedWriter writer;



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
            try {
                // TODO: check if there is overhead introduced by charset encoding
                this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @return int the next parsed integer, or -1 in case of empty file or reader not ready
     * this works since all the expected integer values are positive
     */
    @Override
    int readInt() {
        if(this.mode != MODE.READ){
            return -1;
        }
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
        if(this.mode != MODE.READ){
            return null;
        }
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

    /**
     * @param int in : la codifica del carattere da scrivere su file
     */
    @Override
    void writeInt(int in) {
        if(this.mode != MODE.WRITE){
            return;
        }
        try {
            this.writer.write(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * writes a line to file, it adds the line terminator character \n to the end of the given line
     * @param line
     */
    public void writeLine(String line){
        if(this.mode != MODE.WRITE){
            return;
        }
        try {
            this.writer.append(line + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if(this.mode == MODE.READ){
            try {
                this.reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{  //if(this.mode == MODE.WRITE)
            try {
                this.writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
