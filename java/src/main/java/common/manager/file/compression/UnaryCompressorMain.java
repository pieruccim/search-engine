package common.manager.file.compression;

import java.io.IOException;

public class UnaryCompressorMain {

    public static String printBits(byte b) {
        String ret = "";
        for (int i = 7; i >= 0; i--) {
            int bit = (b >> i) & 1;
            ret+=bit;
        }
        return ret;
    }

    public static void main(String[] args) throws IOException {
        int[] integers = {2, 4, 1, 15}; // Example array of integers to compress

        UnaryCompressor uc = new UnaryCompressor();
        byte[] compressedData = uc.compressIntArray(integers);

        System.out.print("Compressed integers: ");
        // Printing the compressed data as bytes (for demonstration purposes)
        for (byte b : compressedData) {
            System.out.print(printBits(b) + " ");
        }
        System.out.println();

        int[] decompressedIntegers = uc.decompressIntArray(compressedData, integers.length);

        System.out.print("Decompressed integers: ");
        // Print the decompressed integers (for demonstration purposes)
        for (int num : decompressedIntegers) {
            System.out.print(num + " ");
        }
    }

}
