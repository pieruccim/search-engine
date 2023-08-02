package common.manager.file.compression;

import java.io.IOException;

public interface Compressor {

    public byte[] compressIntArray(int[] data) throws IOException;

    public int[] decompressIntArray(byte[] compressedData, int length) throws IOException;

}
