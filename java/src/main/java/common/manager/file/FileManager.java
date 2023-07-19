package common.manager.file;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public abstract class FileManager {

    /*
    * -   metodo per leggere un intero da file
    * -   metodo per leggere una singola linea da file --> questo si fa dove il concetto di linea esiste, quindi solo su file testuali
    * */

    //-   metodo per aprire file dalla collezione [gestire collezione zippata, caricare file in UNICODE non ASCII]
    // al momento della costruzione va specificato se apro in scrittura o lettura
    // che è il costruttore

    public enum MODE{
        READ, WRITE;
    }

    protected MODE mode;
    protected String filePath;
    
    protected void initialSetup(String filePath, MODE mode){
        this.mode = mode;
        this.filePath = filePath;
        // here we have to open the file in the specified mode
        throw new UnsupportedOperationException("Unimplemented method 'initialSetup' for " + this.getClass().toString());

    }

    protected FileManager(){

    }

    public FileManager(String filePath, MODE mode){
        this.initialSetup(filePath, mode);
    }

    public FileManager(String filePath){
        /*
         * @param filePath path of the file to be accessed
         */
        this.initialSetup(filePath, MODE.READ);
    }

    // metodo per leggere un intero da file
    abstract int readInt() throws Exception;

    // method to read int starting from offset
    abstract int readInt(long offset) throws Exception;

    // metodo per scrivere un intero su file
    abstract void writeInt(int in) throws Exception;

    // metodo per chiudere il file
    abstract void close();

}
