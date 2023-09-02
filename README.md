# Search Engine

Search engine that conducts text retrieval opeartions on an extensive compilation of 8.8 million documents available [here](https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020) The project is split upon two primary stages: 

- Document Indexing, which consists in developing data structures and mechanisms required for efficient retrieval
* Query Execution, which focuses in using data structures and queries provided by a user to retrieve most relevant document in the collection

## Performances

In the following plots are displayed performances of the Search Engine both in Conjunctive and Disjunctive queries using TFIDF as scoring function and particular parameters configurations:


| CONJUNCTIVE | DISJUNCTIVE |
| --------- | --------- |
| ![CONJUNCTIVE](https://github.com/pieruccim/search-engine/assets/75124117/f76fe541-d636-4172-ae62-da6182daca24)  |  ![DISJUNCTIVE](https://github.com/pieruccim/search-engine/assets/75124117/09e8a72a-19f7-49dc-94b8-0a98da9473aa) |

## Project Structure and Modules

The Search Engine is composed by the following main modules:

- Common, which contains bean classes and managers used by other modules
+ Preprocessing, which is in charge of cleaning, tokenizing, stemming and stopword removing document and query text
* Indexing, which performs indexing of the collection saving main data structures on disk and executing merging of them
* Query processing, which performs processing of queries using different Document Processors and Scoring Functions


## How to configure and compile modules

### Indexing module

The *Indexer* module can be configured using *config.properties* file, which allows to set the following properties:

| Option                               | Description                                                    |
|--------------------------------------|----------------------------------------------------------------|
| *stopwords*                          | Choose a stopwords list to be removed                          |
| *preprocessing.remove.stopwords*     | Enable or disable stopword removal                             |
| *preprocessing.enable.stemming*      | Enable stemming                                                |
| *invertedIndex.useCompression*       | Enable docids and frequencies compression                      |
| *memory.threshold*                   | Set the memory threshold above which the Block is stored to disk|
| *skipblocks.maxLen*                  | Set the maximum length of a Skip Block                          |


### Query processing module

#### Configuration properties
The *Query processing* module can be configured using *config.properties* file, which allows to set the following properties.

General properties:
| Setting                         | Description                                            |
|---------------------------------|--------------------------------------------------------|
| *query.parameters.nResults*     | Set the number of documents to be retrieved in the corpus |

Document processor and scoring function specific properties:
| Setting                          | Description                                 |
|----------------------------------|---------------------------------------------|
| *scoring.MaxScore.threshold*     | Set MaxScore threshold                      |
| *scoring.BM25.k1*               | Set parameter k1 for BM25 scoring function |
| *scoring.BM25.B*                | Set parameter B for BM25 scoring function  |


Performance properties:
| Setting                                  | Description                                                  |
|------------------------------------------|--------------------------------------------------------------|
| *performance.iterators.useCache*         | Enable cache for Skip Blocks inside an iterator              |
| *performance.iterators.cache.size*       | Set the cache size for Skip Blocks inside an iterator        |
| *performance.iterators.useThreads*       | Enable threads for Skip Blocks inside an iterator            |
| *performance.iterators.threads.howMany*  | Set the number of threads for Skip Blocks inside an iterator |
| *performance.iteratorFactory.cache.enabled* | Enable cache for Posting List Iterators                  |
| *performance.iteratorFactory.cache.size* | Set the cache size for Posting List Iterators               |
| *performance.iteratorFactory.threads.enabled* | Enable threads for Posting List Iterators              |
| *performance.iteratorFactory.threads.howMany* | Set the number of threads for Posting List Iterators   |

#### Compiling properties

On the other hand *Query processing* module can also be compiled using options, that will override the properties inside *config.properties* file. The available options are the followings:
| Option                 | Description                                                |
|------------------------|------------------------------------------------------------|
| *--results*            | Set the number of documents to be returned by the query    |
| *--scoring*            | Set the scoring function between TFIDF and BM25            |
| *--queryType*          | Choose query type between *disjunctive* and *conjunctive*  |
| *--processingType*     | Choose document processor type between *TAAT*, *DAAT*, and *MaxScore* |
| *--stopWords*          | Enable stopwords removal                                   |
| *--wordStemming*       | Enable words stemming                                      |
