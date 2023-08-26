package common.manager.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

import jdk.jshell.spi.ExecutionControl;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

public class TextualFileManager extends FileManager{
    /**
     * @param filePath : if ends with ".tar.gz" it tries to open as targz archive the file at the given path, and searches for a .tsv file inside the archive
     */

    private static String defaultCharset = "UTF-16";

    private BufferedReader reader;
    private BufferedWriter writer;

    private String charset = defaultCharset;

    private TarArchiveInputStream tarGzInput = null;
    private TarArchiveInputStream tarInput = null;

    public TextualFileManager(String filePath){
        super(filePath);
    }

    public TextualFileManager(String filePath, MODE mode) {
        super(filePath, mode);
    }

    public TextualFileManager(String filePath, MODE mode, String charset) {
        this.charset = charset;
        this.initialSetup(filePath, mode);
    }

    @Override
    protected void initialSetup(String filePath, MODE mode){
        this.mode = mode;
        this.filePath = filePath;
        // here we have to open the file in the specified mode
        if(this.mode == MODE.READ){
            if(filePath.endsWith(".tar.gz")){
                try {
                    this.tarGzInput = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(filePath)));
                    
                    TarArchiveEntry entry = this.tarGzInput.getNextTarEntry();
                    //if we want to check the name of the file we are handling: entry.getName().equals(tsvFileName);
                    while(entry != null){
                        if(entry.getName().endsWith(".tsv")){
                            //case in which the tar.gz file directly contains the data file
                            if(this.tarInput != null && !this.tarInput.equals(this.tarGzInput)){
                                // if the object was previously used to open another archive
                                this.tarInput.close();
                            }
                            this.tarInput = this.tarGzInput;
                            //System.out.println("Found entry data file name is " + this.tarInput.getCurrentEntry().getName());
                            break;
                        }else if(entry.getName().endsWith(".tar")){
                            // case in which the tar.gz file contains a .tar archive file in which we have to search for the data file
                            this.tarInput = new TarArchiveInputStream(tarGzInput);
                            while(( entry = this.tarInput.getNextTarEntry()) != null && !entry.getName().endsWith(".tsv")){
                                // iterate over the inner archive until all the elements have been checked and no data file is found
                            }
                            if(entry.getName().endsWith(".tsv")){
                                // case in which I have found a tsv file inside the inner tar archive
                                System.out.println("Found a data file inside the tar whose name is " + entry.getName());
                                break;
                            }else{
                                // if I did not find any data file inside the inner archive, I can close it
                                this.tarInput.close();
                            }

                        }
                        entry = this.tarGzInput.getNextTarEntry();
                    }

                    if(entry == null){
                        throw new FileNotFoundException("No tsv file found in the given archive "+filePath);
                    }

                    System.out.println("data file name in targz archive is " + entry.getName());

                    this.reader = new BufferedReader(new InputStreamReader(this.tarInput, Charset.forName(this.charset)));
                    
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }else{
                try{
                    this.reader = new BufferedReader(new FileReader(filePath, Charset.forName(this.charset)));
                    //this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), TextualFileManager.charset));
                }catch (IOException e){
                    e.printStackTrace();
                }
        }
        }else{  //  if(this.mode == MODE.WRITE)
            try {
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

    @Override
    int readInt(long offset) throws Exception {
        throw new ExecutionControl.NotImplementedException("not implemented yet");
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

        if(this.tarInput != null && ! this.tarInput.equals(this.tarGzInput)){
            try {
                this.tarInput.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        if(this.tarGzInput != null){
            try {
                this.tarGzInput.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
}
