#define _GNU_SOURCE
#include <iostream>
#include <string>
#include <fstream>

//#include <stdio.h>
//#include <stdlib.h>

int stream_test(){
    std::ifstream infile("C:\\programmazione\\search-engine\\test-collection10.tsv");
    
    std::string line;

    std::cout << "infile è aperto? " << infile.is_open() <<std::endl;

    short counter = 0;

    while (getline(infile, line)){
        size_t tab_index = line.find_first_of("\t");        
        int index = std::stoi( line.substr(0, tab_index) );
        std::string document_text = line.substr( tab_index + 1);
        // https://stackoverflow.com/questions/21575310/converting-normal-stdstring-to-utf-8
        // https://www.codeproject.com/Articles/38242/Reading-UTF-8-with-C-streams
        // https://stackoverflow.com/questions/32847064/what-determines-the-normalized-form-of-a-unicode-string-in-c
        // 
        //
        // Libreria per UNICODE normalization:
        // https://icu.unicode.org/
        //
        // std::cout << "Ho letto la riga numero " << counter << " di id " << index << " il cui contenuto è:"<<std::endl<<document_text << std::endl;
        
        if(counter > 4) break;

        counter++;
    }
    return 0;
}

// int fopen_test(){
//     FILE    *textfile;

//     size_t len = 0;
//     size_t read;
//     char * line = NULL;

//     textfile = fopen("test-collection10.tsv", "r");
//     if(textfile == NULL)
//         return 1;

//     //while ((read = getline(&line, &len, textfile)) != -1) {
//     //    printf("Retrieved line of length %zu:\n", read);
//     //    printf("%s", line);
//     //}

//     fclose(textfile);
//     return 0;
// }

int main(){
    return stream_test();
}