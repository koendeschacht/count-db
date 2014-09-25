count-db
========

A fast key-value store (written in Java) that is optimized to handle primitive types (integer/long/double/float) in addition to json serialized objects. 

## Use case

You want to use count-db if you need to write and read billions of counts very efficiëntly from a Java program. Use cases are logging large amounts of user data, counting n-grams for [language models](http://en.wikipedia.org/wiki/Language_model) or building an index to search texts. You don't want to use count-db if you require transactions or my-sql style querying.

## Performance

``be.bagofwords.main.tests.bigrams.BigramTestsMain`` compares the performance of count-db to 3 other key-value stores: [levelDB](https://github.com/google/leveldb), [kyoto cabinet](http://fallabs.com/kyotocabinet/) and [rocksDB](http://rocksdb.org/).  count-db outperforms all three, it is for example 25 times faster then levelDB when  writing 1GB of bigram counts and 8 times faster then levelDB when reading from this 1GB of counts. All tests were performed on an Intel i7-4770 computer using 6GB of memory, with default settings for all key-value stores.


![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/batch_writes.png)

![](https://raw.githubusercontent.com/koendeschacht/count-db/master/doc/batch_reads.png)

## Maven dependency

``` 
<dependency>
    <groupId>be.bagofwords</groupId>
    <artifactId>count-db</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Usage

see [ExampleUsage.java](https://github.com/koendeschacht/count-db/blob/master/src/main/java/be/bagofwords/main/ExampleUsage.java)

``` java
public class ExampleUsage {

    public static void main(String[] args) throws ParseException {
        //create data interface factory that stores all data in /tmp/myData (This factory is wired with spring)
        DataInterfaceFactory dataInterfaceFactory = new EmbeddedDBContextFactory("/tmp/myData").createApplicationContext().getBean(DataInterfaceFactory.class);

        //create databases
        DataInterface<Long> myLogDataInterface = dataInterfaceFactory.createCountDataInterface("myLoginCounts");
        DataInterface<UserObject> myUserDataInterface = dataInterfaceFactory.createDataInterface(DatabaseCachingType.CACHED, "myUsers", UserObject.class, new OverWriteCombinator<UserObject>());

        //write data
        int userId = 12939;
        myLogDataInterface.increaseCount("user_" + userId + "_logged_in");
        myUserDataInterface.write(userId, new UserObject("koen", "deschacht", DateUtils.parseDate("1983-04-12", "yyyy-MM-dd")));

        //flush data
        myLogDataInterface.flush();
        myUserDataInterface.flush();

        //read data
        long numOfLogins = myLogDataInterface.readCount("user_" + userId + "_logged_in");
        UserObject user = myUserDataInterface.read(userId);

        System.out.println("User " + user.getFirstName() + " " + user.getSecondName() + " logged in " + numOfLogins + " times.");
    }
}
```

