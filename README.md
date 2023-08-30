# Search Engine

Search engine that conducts text retrieval opeartions on an extensive compilation of 8.8 million documents available [here](https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020) The project is split upon two primary stages: 

- Document Indexing, which consists in developing data structures and mechanisms required for efficient retrieval
* Query Execution, which focuses in using data structures and queries provided by a user to retrieve most relevant document in the collection  

## Project Structure and Modules

The Search Engine is composed by the following main modules:

- Common, which contains bean classes and managers used by other modules
+ Preprocessing, which is in charge of cleaning, tokenizing, stemming and stopword removing document and query text
* Indexing, which performs indexing of the collection saving main data structures on disk and executing merging of them
* Query processing, which performs processing of queries using different Document Processors and Scoring Functions


## How to configure and compile modules

### Indexing module

The *Indexer* module can be configured using *config.properties* file, which allows to set the following properties:

- *stopwords*, which allows to choose a stopwords list to be removed
- *preprocessing.remove.stopwords*, which allows to enable and disable stopword removal
- *preprocessing.enable.stemming*, which allows to enable stemming
- *invertedIndex.useCompression*, which allows to enable docids and frequencies compression
- *memory.threshold*, which allows to set the memory threshold above which the Block is stored to disk
- *skipblocks.maxLen*, which allows to set the maximum length of a Skip Block

### Query processing module

#### Configuration properties
The *Query processing* module can be configured using *config.properties* file, which allows to set the following properties.

General properties:
- *query.parameters.nResults*, which allows to set the number of documents to be retrieve in the corpus

Document processor and scoring function specific properties:
- *scoring.MaxScore.threshold*, which allows to set MaxScore threshold
- *scoring.BM25.k1* and *scoring.BM25.B*, which allow to set paramters for BM25 scoring function

Performance properties:
- *performance.iterators.useCache* and *performance.iterators.cache.size*, which allow to enable cache for Skip Blocks inside an iterator and set cache size
- *performance.iterators.useThreads* and *performance.iterators.threads.howMany*, which allow to enable threads for Skip Blocks inside an iterator and set threads number
- *performance.iteratorFactory.cache.enabled* and *performance.iteratorFactory.cache.size*, which allow to enable cache for Posting List Iterators and set cache size
- *performance.iteratorFactory.threads.enabled* and *performance.iteratorFactory.threads.howMany*, which allow to enable threads for Posting List Iterators and set threads number

#### Compiling properties

On the other hand *Query processing* module can also be compiled using options, that will override the properties inside *config.properties* file. The available options are the followings:
- *--results*, which allows to set the number of documents to be returned by the query
- *--scoring*, which allows to set the scoring function between TFIDF and BM25
- *--queryType*, which allows to choose query type between *disjunctive* and *conjunctive*
- *--processingType*, which allows to choose document processor typer between *TAAT*, *DAAT* and *MaxScore*
- *--stopWords*, which allows to enable stopwords removal
- *--wordStemming*, which allows to enable words stemming
  
