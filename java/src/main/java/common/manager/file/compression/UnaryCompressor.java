package common.manager.file.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class UnaryCompressor implements Compressor{


    /**
     * @param data array of integers to be decompressed
     * @return byte array of the compressed integers
     * @throws IOException
     */
    @Override
    public byte[] compressIntArray(int[] data){

        int nBits = 0;

        // Retrieve total number of bits to be written: number + 1 (zero bit separator)
        for (int num : data) {
            if (num > 0) {
                nBits += num + 1;
            } else {
                System.out.println("Skipped element <=0 in the list of integers to be compressed");
            }
        }

        // Retrieve total number of bytes needed as ceil of nBits/8
        int nBytes = (nBits + 7) / 8;
        //System.out.println(nBits + " " + nBytes);

        // Initialization of array for the unary representation
        byte[] compressedArray = new byte[nBytes];

        int nextByteToWrite = 0;
        int nextBitToWrite = 0;

        // Compress each integer
        for (int num : data) {
            if (num <= 0) {
                continue;
            }

            for (int j = 0; j < num; j++) {
                // Setting the j-th bit starting from left to 1
                compressedArray[nextByteToWrite] |= (byte) (1 << (7 - nextBitToWrite));

                // Update counters for next bit to write
                nextBitToWrite++;

                // Check if the current byte has been filled
                if (nextBitToWrite == 8) {
                    // New byte must be written as the next byte
                    nextByteToWrite++;
                    nextBitToWrite = 0;
                }
            }

            // Skip a bit since we should encode a 0 (which is the default value) as the last bit
            // of the Unary encoding of the integer to be compressed
            nextBitToWrite++;

            // Check if the current byte has been filled
            if (nextBitToWrite == 8) {
                // New byte must be written as the next byte
                nextByteToWrite++;
                nextBitToWrite = 0;
            }
        }

        return compressedArray;

    }

    /**
     *
     * @param compressedData array of bytes to be decompressed
     * @return array of integers represented by array of bytes
     * @throws IOException
     */
    @Override
    public int[] decompressIntArray(byte[] compressedData, int length) throws IOException{

        int[] decompressedArray = new int[length];

        int toBeReadByte = 0;
        int toBeReadBit = 0;
        int nextInteger = 0;
        int onesCounter = 0;

        // Process each bit
        for(int i=0; i < compressedData.length * 8; i++){

            // Create a byte b where only the bit (i%8)-th is set
            byte b = 0b00000000;
            b |=  (1 << 7 - (i%8));

            // Check if in the byte to be read the bit (i%8)-th is set to 1 or 0
            if((compressedData[toBeReadByte] & b)==0){
                // i-th bit is set to 0

                // Writing the decompressed number in the array of the results
                decompressedArray[nextInteger] = onesCounter;

                // The decompression of a new integer ends with this bit
                nextInteger++;

                if(nextInteger==length)
                    break;

                // resetting the counter of ones for next integer
                onesCounter = 0;

            } else{
                // i-th bit is set to 1

                // Increment the counter of ones
                onesCounter++;
            }

            toBeReadBit++;

            if(toBeReadBit==8){
                toBeReadByte++;
                toBeReadBit=0;
            }
        }

        return decompressedArray;
    }

}
