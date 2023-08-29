package common.manager.file.compression;

import java.io.IOException;

public class DeltaCompressor implements Compressor{

    private VariableByteCompressor variableByteCompressor = null;


    public DeltaCompressor(){
        variableByteCompressor = new VariableByteCompressor();
    }

    /**
     * @param array: integers array to encode with delta encoding
     * @return an integer array encoded with delta encoding
     */
    private static int[] deltaEncoding(int[] array) {
        int[] deltaEncodedArray = new int[array.length];
        deltaEncodedArray[0] = array[0];

        for (int i = 1; i < array.length; i++) {
            deltaEncodedArray[i] = array[i] - array[i - 1];
        }

        return deltaEncodedArray;
    }

    /**
     * @param array of delta encoded integer to decode
     * @return an integer array of decoded integers
     */
    private static int[] deltaDecoding(int[] array) {
        int[] decompressedArray = new int[array.length];
        decompressedArray[0] = array[0];

        for (int i = 1; i < array.length; i++) {
            decompressedArray[i] = decompressedArray[i - 1] + array[i];
        }

        return decompressedArray;
    }

    @Override
    public byte[] compressIntArray(int[] data) throws IOException {

        // Apply Delta Encoding to the array
        int[] deltaEncodedArray = deltaEncoding(data);

        // Apply Variable Byte Encoding to the delta encoded array
        byte[] compressedData = variableByteCompressor.compressIntArray(deltaEncodedArray);

        return compressedData;
    }

    @Override
    public int[] decompressIntArray(byte[] compressedData, int length) throws IOException {

        // Perform Variable Byte Decoding on the compressed data
        int[] deltaEncodedArray = variableByteCompressor.decompressIntArray(compressedData, length);

        // Perform Delta Decoding on the Variable Byte Decoded array
        int[] decompressedArray = deltaDecoding(deltaEncodedArray);

        return decompressedArray;
    }
}
