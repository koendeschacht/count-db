count-db performance
====================

All tests are performed on a computer with an intel i7-4770 processor, a SATA 6 Gb/s 7200 rpm hard disk and 32GB of RAM, of which a maximum of 10GB is used by the java virtual machine. All tests use 8 threads to write or read values in parallel.

## Databases

The purpose of this test was to measure performance of different key-value stores *from a java program*, and thus jni bindings are used for [levelDB](https://github.com/fusesource/leveldbjni), [kyoto cabinet](http://fallabs.com/kyotocabinet/javadoc/) and [rocksDB](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics). We used default parameters for all databases. 

## Reading and writing bigram counts

The first test in [BigramTestsMain.java](https://github.com/koendeschacht/count-db/blob/master/src/main/java/be/bagofwords/main/tests/bigrams/BigramTestsMain.java) reads the database dump of the English wikipedia and counts all bigrams. First a number (varying between 1M and 256M) of bigrams is written to the database, and then the same number of bigrams is read from this database. 

### Writing

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/write_bigram_counts.png)

| million items | levelDB   | count-db  | kyoto cabinet | rocksDB   |
|---------------|-----------|-----------|---------------|-----------|
| 1             | 1.82E+005 | 1.75E+006 | 1.53E+006     | 1.13E+005 |
| 2             | 1.49E+005 | 3.77E+006 | 1.76E+006     | 1.15E+005 |
| 4             | 1.84E+005 | 5.30E+006 | 1.73E+006     | 1.18E+005 |
| 8             | 1.68E+005 | 5.74E+006 | 1.69E+006     | 1.23E+005 |
| 16            | 1.60E+005 | 4.60E+006 | 1.23E+005     | 1.29E+005 |
| 32            | 1.50E+005 | 5.54E+006 | 4.20E+004     | 1.35E+005 |
| 64            | 1.42E+005 | 5.62E+006 | 2.77E+004     | 1.38E+005 |
| 128           | 1.37E+005 | 6.02E+006 | 2.68E+004     | 1.38E+005 |
| 256           | 1.37E+005 | 6.26E+006 |               | 1.40E+005 |

### Reading

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/read_bigram_counts.png)

| million items | levelDB   | count-db  | kyoto cabinet | rocksDB   |
|---------------|-----------|-----------|---------------|-----------|
| 1             | 1.29E+006 | 4.19E+006 | 2.13E+006     | 7.24E+005 |
| 2             | 1.11E+006 | 1.08E+007 | 2.07E+006     | 7.55E+005 |
| 4             | 1.04E+006 | 1.09E+007 | 1.99E+006     | 6.48E+005 |
| 8             | 9.73E+005 | 1.24E+007 | 1.95E+006     | 5.31E+005 |
| 16            | 9.31E+005 | 1.24E+007 | 8.21E+004     | 5.85E+005 |
| 32            | 9.02E+005 | 1.22E+007 | 5.02E+004     | 6.18E+005 |
| 64            | 9.18E+005 | 1.16E+007 | 5.09E+004     | 5.39E+005 |
| 128           | 9.60E+005 | 1.23E+007 | 5.11E+004     | 5.18E+005 |
| 256           | 8.90E+005 | 1.12E+007 |               | 4.93E+005 |

Note that the test that writes and reads 256M of counts was not finished for kyoto cabinet because it took too long.

## Reading and writing bigram counts in parallel

The second test in [BigramTestsMain.java](https://github.com/koendeschacht/count-db/blob/master/src/main/java/be/bagofwords/main/tests/bigrams/BigramTestsMain.java) measures the performance of reading and writing bigram counts in parallel. First 128M bigrams is written to the database. The speed of writing these bigrams is not measured. Consecutively 4 threads are started that write the same number of counts to the database, and in parallel, 4 threads are started that read the same number of bigrams for the database. The performance of these threads is measured:

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/parallel_performance.png)

| action | levelDB   | count-db  | rocksDB   |
|--------|-----------|-----------|-----------|
| read   | 2.02E+005 | 2.27E+006 | 2.40E+005 |
| write  | 1.24E+005 | 2.10E+006 | 1.03E+005 |


The tests for for kyoto cabinet are not included because these took too long to finish.

## Reading and writing java objects

The third test in [BigramTestsMain.java](https://github.com/koendeschacht/count-db/blob/master/src/main/java/be/bagofwords/main/tests/bigrams/BigramTestsMain.java) measures the performance of reading and writing java objects. This test is identical to the first test, but instead of a long number, a java object with the total count is stored. The performance for writing and reading of 128M of bigrams was:

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/java_objects_performance.png)

| action | levelDB   | count-db  | kyoto cabinet | rocksDB   |
|--------|-----------|-----------|---------------|-----------|
| read   | 7.39E+005 | 4.24E+006 | 6.61E+004     | 4.53E+005 |
| write  | 7.88E+004 | 2.60E+006 | 5.08E+003     | 9.55E+004 |


## Reading and writing uniform counts

The three previous tests all use bigram counts. This data follows a [power law](http://en.wikipedia.org/wiki/Power_law) distribution. Although this type of distribution is observed with many types of real-world data, not all data follows this distribution. As an additional test, in [UniformDataTestsMain.java](https://github.com/koendeschacht/count-db/blob/master/src/main/java/be/bagofwords/main/tests/uniform/UniformDataTestsMain.java), we chosen random values from a uniform distribution between 0 and 1000000. First 128M of counts is written to the database, and then the same number of counts is read.

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/uniform_counts_performance.png)

| action | levelDB   | count-db  | kyoto cabinet | rocksDB   |
|--------|-----------|-----------|---------------|-----------|
| read   | 8.32E+005 | 2.21E+007 | 1.93E+006     | 6.29E+005 |
| write  | 1.56E+005 | 1.93E+007 | 2.01E+006     | 1.07E+005 |
