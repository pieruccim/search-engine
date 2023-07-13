import common.bean.DocumentIndex;
import org.junit.jupiter.api.Test;

public class DocumentIndexTest {
    @Test
    void constructorTest() {
        DocumentIndex documentIndex = new DocumentIndex();
    }

    @Test
    void addInformationTest(){
        DocumentIndex documentIndex = new DocumentIndex();
        documentIndex.addInformation(47, 1, 1);
        System.out.println(documentIndex.toString(1));
        System.out.println(documentIndex.toString(2));
    }
}
