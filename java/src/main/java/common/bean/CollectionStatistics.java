package common.bean;

public class CollectionStatistics {
    private int totalDocuments;
    private double averageDocumentLength;

    public CollectionStatistics(int totalDocuments, double averageDocumentLength) {
        this.totalDocuments = totalDocuments;
        this.averageDocumentLength = averageDocumentLength;
    }

    public int getTotalDocuments() {
        return totalDocuments;
    }

    public double getAverageDocumentLength() {
        return averageDocumentLength;
    }

    public void setTotalDocuments(int totalDocuments) {
        this.totalDocuments = totalDocuments;
    }

    public void setAverageDocumentLength(double averageDocumentLength) {
        this.averageDocumentLength = averageDocumentLength;
    }

    @Override
    public String toString() {
        return "CollectionStatistics{" +
                "totalDocuments=" + totalDocuments +
                ", averageDocumentLength=" + averageDocumentLength +
                '}';
    }
}
