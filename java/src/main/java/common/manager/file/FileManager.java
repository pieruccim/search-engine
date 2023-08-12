package common.manager.file;

import config.ConfigLoader;

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

    public static void checkExistingOutputFiles() throws IOException {
        String outputPath = String.valueOf(ConfigLoader.getProperty("data.output.path"));
        boolean overrideOutputFiles = ConfigLoader.getPropertyBool("output.files.override");

        if (overrideOutputFiles) {
            if (isDirectoryEmpty(outputPath)) {
                emptyPath(new File(outputPath));
            } else {
                System.out.println("The output directory is not empty. Do you want to override output files? (yes/no)");
                Scanner scanner = new Scanner(System.in);
                String response = scanner.nextLine().trim().toLowerCase();
                scanner.close();

                if (response.equals("yes")) {
                    emptyPath(new File(outputPath));
                } else {
                    System.out.println("Output directory is not empty, indexing aborted. Exiting...");
                    System.exit(0);
                }
            }
        } else {
            if (!isDirectoryEmpty(outputPath)) {
                throw new IOException("Output directory is not empty. Please rename the existing 'output' folder" +
                        " to distinguish from the new one being created.");
            }
        }
    }

    static boolean isDirectoryEmpty(String path) {
        File directory = new File(path);
        if (directory.exists() && directory.isDirectory()) {
            return directory.list().length == 0;
        }
        //treat non-existent directory as empty
        return true;
    }
    public static void emptyPath(File file) throws IOException {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File subFile : files) {
                    emptyPath(subFile);
                }
            }
        }

        if (file.exists() && !file.delete()) {
            throw new IOException("Failed to delete file: " + file.getAbsolutePath());
        }
    }

    abstract void close();
}
