import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class InvertedIndexTest {
    @Test
    void addPostingsTest() {

        //InvertedIndex invertedIndex = new InvertedIndex();

        HashMap<String, Integer> termCounter1 = new HashMap<>();
        termCounter1.put("abc",4);
        HashMap<String, Integer> termCounter2 = new HashMap<>();
        termCounter2.put("abc",11);

        //invertedIndex.addPostings(1, termCounter1);
        //invertedIndex.addPostings(2, termCounter2);

        //System.out.println(invertedIndex.toString("abc"));
    }
}
