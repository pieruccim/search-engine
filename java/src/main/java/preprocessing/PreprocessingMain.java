package preprocessing;

import javafx.util.Pair;

import java.util.Arrays;

public class PreprocessingMain {
    public static void main(String[] args) {
        String line = "1\tThe Manhattan Project and its atomic bomb helped bring an end to World War II. Its legacy of peaceful uses of atomic energy continues to have an impact on history and science.\n";

        Pair<Integer, String> lineFormatted = Preprocessor.parseLine(line);
        int docno = lineFormatted.getKey();
        String text = lineFormatted.getValue();

        System.out.println("Original Text: " + line);
        System.out.println("Docno: " + docno + "\nText: " + text);

        String[] tokens = Preprocessor.processText(text, false);

        System.out.print("Tokens: [");

        for (int i = 0; i < tokens.length; i++) {
            if(i == tokens.length-1) {
                System.out.print(tokens[i] + "]");
                continue;
            }
            System.out.print(tokens[i] + "; ");
        }
            
    }
}
