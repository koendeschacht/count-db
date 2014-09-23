count-db
========

A fast key-value store (written in Java) that is optimized to handle primitive types (integer/long/double/float) in addition to json serialized objects. 

## Use case

You want to use count-db if you need to write and read billions of counts very efficiëntly from a Java program. Use cases are logging large amounts of user data, counting n-grams for [language models](http://en.wikipedia.org/wiki/Language_model) or building an index to search texts.

## Performance

The class ``be.bagofwords.main.tests.bigrams.BigramTestsMain`` compares the performance of count-db to other key-value stores. It shows that count-db is 25 times as fast as [levelDB](https://github.com/google/leveldb) for writing 1GB of bigram counts and 8 times as fast as levelDB in reading bigram counts from this 1GB of counts.


![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/batch_writes.png)

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/batch_reads.png)


## Usage

Creating databases 
```
MemoryManager memoryManager = new MemoryManager() ; //should be unique in application, can be wired through spring
CachesManager cachesManager = new CachesManager() ; //should be unique in application, can be wired through spring
OpenFilesManager openFilesManager = new OpenFilesManager() ; //should be unique in application, can be wired through spring
DataInterfaceFactory dataInterfaceFactory = new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager,"/tmp/myData"); 
DataInterface<Long> myLogDataInterface = dataInterfaceFactory.createCountDataInterface("myCounts");
DataInterface<UserObject> myUserDataInterface = dataInterfaceFactory.createDataInterface(DatabaseCachingType.CACHED, "myObjects", UserObject.class, new OverWriteCombinator<UserObject>());
...
```

Writing 
```
myLogDataInterface.increaseCount("user_koen_logged_in");
myUserDataInterface.write(12939, new UserObject("koen", "deschacht")) ;
```

Reading
```
long numOfLogins = myLogDataInterface.readCount("user_koen_logged_in");
UserObject koenUser = myUserDataInterface.read(12939) ;
```

