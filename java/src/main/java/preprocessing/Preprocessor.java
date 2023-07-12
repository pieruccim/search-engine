package preprocessing;

public class Preprocessor {

    /* riceve una linea (nella forma <docno>\t<text>\n) della collezione,
     controlla se la linea è malformata (nel caso lancia eccezione), infine ritorna tupla con pid e text */

    /*
    * controlli linea malformata:
    *   -   linea vuota
    *   -   il <docno> non è parsabile come intero
    *   -   il campo <text> è vuoto
    *   -   non è presente \t
    *   -   carattere non formattati correttamente
    * */

    /*
    * riceve il testo del documento -> metodi:
    *
    *   - rimuove puntualizzazione e stop-word
    *   - trasforma in lower case
    *   - stemming
    *   - esegue tokenizzazione
    *
    * ritorna lista di token
    * */
}
