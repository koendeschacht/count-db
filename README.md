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

see [ExampleUsage.java](https://github.com/koendeschacht/count-db/blob/master/src/main/java/be/bagofwords/main/ExampleUsage.java)

```
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

