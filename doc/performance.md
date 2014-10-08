count-db performance
====================

All tests are performed on a computer with an intel i7-4770 processor, a SATA 6 Gb/s 7200 rpm hard disk and 32GB of RAM, of which a maximum of 10GB is used by the java virtual machine. All tests use 8 threads to write or read values in parallel.

## Databases

The purpose of this test was to measure performance of different key-value stores *from a java program*, and thus jni bindings are used for [levelDB](https://github.com/fusesource/leveldbjni), [kyoto cabinet](http://fallabs.com/kyotocabinet/javadoc/) and [rocksDB](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics). We used default parameters for all databases. 

For rocksDB we experimented with using a UInt64AddOperator [merge operator](https://github.com/facebook/rocksdb/wiki/Merge-Operator) when writing long counts. These results are indicated below with an asterix (rocksDB*). Since merge operators are not yet supported in the rocksDB jni binding, we added the following hack to [rocksjni.cc](https://github.com/facebook/rocksdb/blob/master/java/rocksjni/rocksjni.cc)

```
void Java_org_rocksdb_RocksDB_open(
    JNIEnv* env, jobject jdb, jlong jopt_handle, jstring jdb_path) {
  auto opt = reinterpret_cast<rocksdb::Options*>(jopt_handle);
  rocksdb::DB* db = nullptr;
  const char* db_path = env->GetStringUTFChars(jdb_path, 0);
  //BEGIN HACK
  if(strstr(db_path,"_long_count_")) {
    opt->merge_operator = MergeOperators::CreateUInt64AddOperator();
  }
  //END HACK
  rocksdb::Status s = rocksdb::DB::Open(*opt, db_path, &db);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (s.ok()) {
    rocksdb::RocksDBJni::setHandle(env, jdb, db);
    return;
  }
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}
```

## Reading and writing bigram counts

The first test in [BigramTestsMain.java](https://github.com/koendeschacht/count-db/blob/master/src/main/java/be/bagofwords/main/tests/bigrams/BigramTestsMain.java) reads the database dump of the English wikipedia and counts all bigrams. First a number (varying between 1M and 256M) of bigrams is written to the database, and then the same number of bigrams is read from this database. 

### Writing

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/write_bigram_counts.png)

| million counts | levelDB   | count-db  | kyoto cabinet | rocksdb   | rocksdb*  |
|----------------|-----------|-----------|---------------|-----------|-----------|
| 1              | 1.95E+005 | 1.16E+006 | 1.66E+006     | 1.38E+005 | 1.35E+005 |
| 2              | 2.03E+005 | 2.10E+006 | 1.65E+006     | 1.43E+005 | 1.47E+005 |
| 4              | 1.97E+005 | 3.32E+006 | 1.67E+006     | 1.38E+005 | 1.44E+005 |
| 8              | 1.84E+005 | 3.92E+006 | 1.63E+006     | 1.44E+005 | 1.47E+005 |
| 16             | 1.68E+005 | 5.18E+006 | 1.09E+005     | 1.48E+005 | 1.49E+005 |
| 32             | 1.60E+005 | 5.47E+006 | 4.10E+004     | 1.52E+005 | 1.51E+005 |
| 64             | 1.56E+005 | 5.58E+006 | 2.77E+004     | 1.53E+005 | 1.53E+005 |
| 128            | 1.47E+005 | 3.62E+006 | 2.10E+004     | 1.51E+005 | 1.52E+005 |
| 256            | 1.49E+005 | 4.90E+006 |               | 1.56E+005 | 1.58E+005 |

### Reading

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/read_bigram_counts.png)

| million counts | levelDB   | count-db  | kyoto cabinet  | rocksDB   | rocksDB*  |
|----------------|-----------|-----------|----------------|-----------|-----------|
| 1              | 1.12E+006 | 6.58E+006 | 2.12E+006      | 5.08E+005 | 5.18E+005 |
| 2              | 1.02E+006 | 7.90E+006 | 2.07E+006      | 6.03E+005 | 6.22E+005 |
| 4              | 9.66E+005 | 7.48E+006 | 2.00E+006      | 5.15E+005 | 5.44E+005 |
| 8              | 9.14E+005 | 8.09E+006 | 1.93E+006      | 6.20E+005 | 6.29E+005 |
| 16             | 8.83E+005 | 7.32E+006 | 8.16E+004      | 4.31E+005 | 4.37E+005 |
| 32             | 8.65E+005 | 7.16E+006 | 5.04E+004      | 5.60E+005 | 4.60E+005 |
| 64             | 8.48E+005 | 7.07E+006 | 5.09E+004      | 4.39E+005 | 4.48E+005 |
| 128            | 8.99E+005 | 7.31E+006 | 6.22E+004      | 4.55E+005 | 5.91E+005 |
| 256            | 8.24E+005 | 6.65E+006 |                | 4.60E+005 | 4.63E+005 |

Note that the test that writes and reads 256M of counts was not finished for kyoto cabinet because it took too long.

## Reading and writing bigram counts in parallel

The second test in [BigramTestsMain.java](https://github.com/koendeschacht/count-db/blob/master/src/main/java/be/bagofwords/main/tests/bigrams/BigramTestsMain.java) measures the performance of reading and writing bigram counts in parallel. First 128M bigrams is written to the database. The speed of writing these bigrams is not measured. Consecutively 4 threads are started that write the same number of counts to the database, and in parallel, 4 threads are started that read the same number of bigrams for the database. The performance of these threads is measured:

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/parallel_performance.png)

| action | leveldb   | count-db  | rocksdb   | rocksdb*  |
|--------|-----------|-----------|-----------|-----------|
| read   | 2.17E+005 | 1.26E+006 | 2.52E+005 | 2.51E+005 |
| write  | 1.33E+005 | 1.26E+006 | 1.21E+005 | 1.22E+005 |

We did not include the tests for kyoto cabinet because these took too long.

## Reading and writing java objects

The third test in [BigramTestsMain.java](https://github.com/koendeschacht/count-db/blob/master/src/main/java/be/bagofwords/main/tests/bigrams/BigramTestsMain.java) measures the performance of reading and writing java objects. This test is identical to the first test, but instead of a count, a java object, with the first word, the second word and the total count, is stored. We only show results here for 128M of bigrams.

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/java_objects_performance.png)

| action | leveldb   | count-db  | kyoto     | rocksdb   |
|--------|-----------|-----------|-----------|-----------|
| read   | 6.28E+005 | 3.83E+006 | 6.61E+004 | 4.37E+005 |
| write  | 7.73E+004 | 1.14E+006 | 5.08E+003 | 8.41E+004 |


## Reading and writing uniform counts

The three previous tests all use bigram counts. This data follows a [power law](http://en.wikipedia.org/wiki/Power_law) distribution. Although this type of distribution is observed with many types of real-world data, not all data follows this distribution. As an additional test, in [UniformDataTestsMain.java](https://github.com/koendeschacht/count-db/blob/master/src/main/java/be/bagofwords/main/tests/uniform/UniformDataTestsMain.java), we chosen random values from a uniform distribution between 0 and 1000000. First 128M of counts is written to the database, and then the same number of counts is read.

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/uniform_counts_performance.png)

| action | leveldb   | count-db  | kyoto     | rocksdb   | rocksdb*  |
|--------|-----------|-----------|-----------|-----------|-----------|
| reads  | 8.32E+005 | 1.04E+007 | 1.93E+006 | 6.29E+005 | 5.90E+005 |
| writes | 1.56E+005 | 5.59E+006 | 2.01E+006 | 1.07E+005 | 1.08E+005 |
