package common.manager;

public class BinaryFileManager extends FileManager{

    public BinaryFileManager(String filePath) {
        super(filePath);
    }

    public BinaryFileManager(String filePath, MODE mode) {
        super(filePath, mode);
    }

    @Override
    public int readInt() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'readInt'");
    }

    @Override
    public void writeInt(int in) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'writeInt'");
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

}
