package common.manager.file.compression;

import java.io.IOException;
import java.util.ArrayList;

import static java.lang.Math.log;

public class VariableByteCompressor implements Compressor{

    /**
     *
     * @param in: input integer
     * @return byte representation of the inout integer in variable byte encoding
     */
    private static byte[] integerCompression(int in){

        // In input integer is 0 return byte with all zeros
        if(in == 0){
            return new byte[]{(byte) 0x80};
        }

        // Retrieve the number of bytes needed
        int numBytes = (int) (log(in) / log(128)) + 1;

        // Allocate the output byte array
        byte[] output = new byte[numBytes];

        // For each position set the correct bytes and divide the number by 128 to prepare the next setting
        for(int position = numBytes - 1; position >= 0; position--){
            output[position] = (byte) (in % 128);
            in /= 128;
        }

        // Set the most significant bit of the least significant byte to 1
        output[numBytes - 1] += 128;
        return output;
    }

    /**
     *
     * @param array: input integers array
     * @return bytes representing the input integers in variable byte encoding
     * @throws IOException
     */
    @Override
    public byte[] compressIntArray(int[] array) throws IOException {
        ArrayList<Byte> compressedArray = new ArrayList<>();


        // For each element to be compressed
        for(int number: array){
            // Perform the compression and append the compressed output to the byte list
            for(byte elem: integerCompression(number))
                compressedArray.add(elem);
        }

        // Transform the arraylist to an array
        byte[] output = new byte[compressedArray.size()];
        for(int i = 0; i < compressedArray.size(); i++)
            output[i] = compressedArray.get(i);

        return output;
    }

    /**
     *
     * @param compressedData: bytes representing integers in variable byte encoding
     * @param n: total amount of integers to be decoded
     * @return integers array that has been decoded from input byte array
     * @throws IOException
     */
    @Override
    public int[] decompressIntArray(byte[] compressedData, int n) throws IOException {

        int[] decompressedArray = new int[n];

        // integer that I'm processing
        int decompressedNumber = 0;

        // count of the processed numbers (used also as a pointer in the output array)
        int alreadyDecompressed = 0;

        for(byte elem: compressedData){
            if((elem & 0xff) < 128)
                // not the termination byte, shift the actual number and insert the new byte
                decompressedNumber = 128 * decompressedNumber + elem;
            else{
                // termination byte, remove the 1 at the MSB and then append the byte to the number
                decompressedNumber = 128 * decompressedNumber + ((elem - 128) & 0xff);

                // save the number in the output array
                decompressedArray[alreadyDecompressed] = decompressedNumber;

                // increase the number of processed numbers
                alreadyDecompressed ++;

                //reset the variable for the next number to decompress
                decompressedNumber = 0;
            }
        }

        return decompressedArray;
    }

}
