package preprocessing;
import javafx.util.Pair;
import opennlp.tools.stemmer.PorterStemmer;

public class Preprocessor {

    private static final String[] stopwordsList = {"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "it", "it's", "its", "itself", "let's", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she", "she'd", "she'll", "she's", "should", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "we'd", "we'll", "we're", "we've", "were", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "would", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"};


    /**
     * Receives a line in the form "<docno>\t<text>\n", controls if it is malformed (throws exception)
     * and otherwise parses it.
     *
     * Malformed line/document:
     *  -   empty line
     *  -   empty Docno
     *  -   not parsable Docno
     *  -   empty Text
     *  -   invalid line format
     *
     * @param line The line to be parsed.
     * @return A Pair containing Docno and Text.
     */

     static public Pair<Integer,String> parseLine(String line) throws IllegalArgumentException{

        if (line.isEmpty()){
            // Empty line
            throw new IllegalArgumentException("Empty line");
        }

        String[] parts = line.trim().split("\t");

        //System.out.println("Document input parts: " + parts.length + "\n docNo: " + parts[0] + "\n text: " + parts[1]);

        if (parts.length == 2) {

            if (parts[0].isEmpty()){
                // Empty docno
                throw new IllegalArgumentException("DocNo is empty");
            }

            int docno;
            // Not parsable docno
            try {
                docno = Integer.parseInt(parts[0]);

            } catch (Exception e){
                throw new IllegalArgumentException("DocNo not parsable as int");
            }

            if (parts[1].isEmpty()){
                // Empty text
                throw new IllegalArgumentException("Text is empty");
            }
            String text = parts[1];

            return new Pair<>(docno, text);
        }

        // Invalid line format
        throw new IllegalArgumentException("Invalid line format");
    }

    static private String convertToLowercase(String text){
        // Convert text string to lowercase
        return text.toLowerCase();
    }

    static private String removeBadCharacters(String text){
        String pattern = "[^a-z0-9]";
        return text.replaceAll(pattern, " ");
    }

    static private String removeStopwords(String text){

        String pattern = "\\b(" + String.join("|", stopwordsList) + ")\\b";
        return text.replaceAll(pattern, "");
    }

    static private String removePunctuation(String text){
        // Match any punctuation character with regular expression
        return text.replaceAll("\\p{Punct}", "");
    }

    static private String[] tokenizeText(String text){
        // Space-based tokenization using regular expression
        return text.trim().split("\\s+");
    }

    static private String[] executeStemming(String[] tokens){
         PorterStemmer stemmer = new PorterStemmer();

        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = stemmer.stem(tokens[i]);
        }
        return tokens;
    }

    /**
     * Executes pre-processing steps over given text:
     *  -   conversion to lowercase
     *  -   stop-words removing
     *  -   punctuation removing
     *  -   tokenization
     *  -   stemming
     *
     * @param text The text to be pre-processed.
     * @param debug Optional debug enabling.
     * @return Pre-processed and tokenized text.
     */

    static public String[] processText(String text, boolean debug){

        text = convertToLowercase(text);
        text = removeBadCharacters(text);
        text = removeStopwords(text);
        text = removePunctuation(text);

        if (debug){
            System.out.println("Lowercase Text: " + text);
            System.out.println("Text without bad characters: " + text);
            System.out.println("Text without Stopwords: " + text);
            System.out.println("Text without Punctuation: " + text);
        }

        String[] tokens = tokenizeText(text);
        executeStemming(tokens);
        return tokens;
    }
}
