import common.bean.Vocabulary;
import org.junit.jupiter.api.Test;

public class VocabularyTest {
    @Test
    void constructorTest() {
        Vocabulary vocabulary = new Vocabulary();
    }

    @Test
    void addInformationTest(){
        Vocabulary vocabulary = new Vocabulary();

        vocabulary.addInformation("abc", 1);
        vocabulary.addInformation("abc", 2);
        vocabulary.addInformation("def", 1);
        vocabulary.addInformation("def", 3);
        vocabulary.addInformation("def", 8);

        System.out.println(vocabulary.toString("abc"));
        System.out.println(vocabulary.toString("def"));
        System.out.println(vocabulary.toString("ghi"));
    }
}
