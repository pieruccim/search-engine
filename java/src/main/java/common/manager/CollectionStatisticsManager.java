package common.manager;

import common.bean.CollectionStatistics;
import common.manager.file.FileManager;
import common.manager.file.FileManager.*;
import common.manager.file.TextualFileManager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CollectionStatisticsManager {

    protected TextualFileManager textualFileManager;
    protected String statsFilePath;

    public CollectionStatisticsManager(String collectionStatisticsFilePath){
        this.statsFilePath = collectionStatisticsFilePath;
    }

    protected void openFile(MODE mode) throws IOException {

        Files.createDirectories(Paths.get(statsFilePath).getParent());
        
        File f = new File(this.statsFilePath);

        if (mode == MODE.WRITE) {
            if (f.exists()) {
                emptyPath(f);
            }
            
            this.textualFileManager = new TextualFileManager(this.statsFilePath, MODE.WRITE, "UTF-8");
        } else {
            if (!f.exists()) {
                throw new IOException("File '" + this.statsFilePath + "' doesn't exist");
            }
            this.textualFileManager = new TextualFileManager(this.statsFilePath, MODE.READ, "UTF-8");
        }
    }

    private void emptyPath(File file) throws IOException {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File subFile : files) {
                    emptyPath(subFile);
                }
            }
        }

        if (!file.delete()) {
            throw new IOException("Failed to delete file: " + file.getAbsolutePath());
        }
    }

    public void saveCollectionStatistics(CollectionStatistics collectionStatistics) {
        try {
            this.openFile(MODE.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String statsLine = collectionStatistics.getTotalDocuments() + "\t" + collectionStatistics.getAverageDocumentLength();
        this.textualFileManager.writeLine(statsLine);

        this.textualFileManager.close();
    }

    public CollectionStatistics readCollectionStatistics() throws IOException {
        this.openFile(MODE.READ);
        String line = this.textualFileManager.readLine();
        if (line != null) {
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                int totalDocuments = Integer.parseInt(parts[0]);
                double averageDocumentLength = Double.parseDouble(parts[1]);
                return new CollectionStatistics(totalDocuments, averageDocumentLength);
            }
        }

        this.textualFileManager.close();

        throw new IOException("Cannot read statistics");
        //return null if unable to read or parse collection statistics
        //return null;
    }

    
}
