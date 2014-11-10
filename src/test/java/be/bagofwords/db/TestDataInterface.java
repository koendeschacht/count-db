package be.bagofwords.db;

import be.bagofwords.db.combinator.OverWriteCombinator;
import be.bagofwords.db.helper.TestObject;
import be.bagofwords.db.helper.UnitTestContextLoader;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.test.context.ContextConfiguration;

import java.util.*;

@RunWith(value = Parameterized.class)
@ContextConfiguration(loader = UnitTestContextLoader.class)
public class TestDataInterface extends BaseTestDataInterface {

    public TestDataInterface(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Test
    public void sanityCheck() throws Exception {
        Random random = new Random(1204);
        DataInterface<TestObject> dataInterface = dataInterfaceFactory.createDataInterface(type, "sanityCheck", TestObject.class, new OverWriteCombinator<TestObject>());
        dataInterface.dropAllData();
        writeRandomObjects(dataInterface, 200, random);
        TestObject randomObj = createRandomObject(random);
        dataInterface.write("obj", randomObj);
        writeRandomObjects(dataInterface, 200, random);
        TestObject readObj = dataInterface.read("obj");
        Assert.assertEquals(randomObj, readObj);
    }

    @Test
    public void testDropData() throws Exception {
        Random random = new Random();
        TestObject randomObj = createRandomObject(random);
        DataInterface<TestObject> dataInterface = dataInterfaceFactory.createDataInterface(type, "testDropData", TestObject.class, new OverWriteCombinator<TestObject>());
        dataInterface.write("obj", randomObj);
        dataInterface.flush();
        TestObject readObj = dataInterface.read("obj");
        Assert.assertEquals(randomObj, readObj);
        dataInterface.dropAllData();
        Assert.assertNull(dataInterface.read("obj"));
    }

    @Test
    public void testIterator() throws Exception {
        int numOfExamples = 100;
        DataInterface<Integer> dataInterface = dataInterfaceFactory.createDataInterface(type, "testIterator", Integer.class, new OverWriteCombinator<Integer>());
        dataInterface.dropAllData();
        for (int i = 0; i < numOfExamples; i++) {
            dataInterface.write(i, i);
        }
        CloseableIterator<KeyValue<Integer>> it = dataInterface.iterator();
        while (it.hasNext()) {
            KeyValue<Integer> next = it.next();
            long key = next.getKey();
            Integer value = next.getValue();
            Assert.assertEquals(value.intValue(), (int) key);
        }
        it.close();
    }

    @Test
    public void testRandomValues() throws Exception {
        long numOfExamples = 200;
        DataInterface<TestObject> dataInterface = dataInterfaceFactory.createDataInterface(type, "testRandomValues", TestObject.class, new OverWriteCombinator<TestObject>());
        dataInterface.dropAllData();
        for (int i = 0; i < numOfExamples; i++) {
            dataInterface.write(Integer.toString(i), new TestObject(i, Integer.toString(i)));
        }
        dataInterface.flush();
        for (int i = 0; i < numOfExamples; i++) {
            TestObject obj = dataInterface.read(Integer.toString(i));
            Assert.assertNotNull(obj);
            Assert.assertEquals(i, obj.getValue1());
        }
    }

    @Test
    public void testCountsWithPause() throws Exception {
        long numOfExamples = 10000;
        DataInterface<Long> dataInterface = createCountDataInterface("testCountsWithPause");
        dataInterface.dropAllData();
        for (int i = 0; i < numOfExamples; i++) {
            dataInterface.write((long) i, 2l * i);
        }
        dataInterface.flush();
        Utils.threadSleep(1000); //Give the cleanup threads time to run
        for (int i = 0; i < numOfExamples; i++) {
            long val = dataInterface.readCount(i);
            if (val != 2l * i) {
                dataInterface.readCount(i);
            }
            Assert.assertEquals(2l * i, val);
        }
    }

    @Test
    public void testWriteCountMap() throws Exception {
        int numOfExamples = 100;
        DataInterface<Long> dataInterface1 = createCountDataInterface("testWriteCountMap");
        dataInterface1.dropAllData();
        List<KeyValue<Long>> values = new ArrayList<>();
        Map<Integer, Long> expectedValues = new HashMap<>();
        Random random = new Random();
        for (int i = 0; i < numOfExamples; i++) {
            long nextVal = random.nextLong();
            values.add(new KeyValue<>(i, nextVal));
            expectedValues.put(i, nextVal);
        }
        dataInterface1.write(values.iterator());
        dataInterface1.flush();
        for (int i = 0; i < numOfExamples; i++) {
            long nextVal = dataInterface1.readCount(i);
            Assert.assertEquals(expectedValues.get(i).longValue(), nextVal);
        }

    }

    @Test
    public void testDeleteValue() throws Exception {
        int numOfExamples = 100;
        DataInterface<Long> db = createCountDataInterface("testDeleteValue");
        db.dropAllData();
        for (int i = 0; i < numOfExamples; i++) {
            db.increaseCount(Integer.toString(i));
        }
        for (int i = 0; i < numOfExamples; i += 2) {
            db.write(Integer.toString(i), null);
        }
        db.flush();
        for (int i = 0; i < numOfExamples; i++) {
            Long count = db.readCount(Integer.toString(i));
            if (i % 2 == 0) {
                Assert.assertEquals(0, count.intValue());
            } else {
                Assert.assertEquals(1, count.intValue());
            }
        }
    }

    @Test
    public void testApproximateSize() throws Exception {
        int numOfExamples = 1000;
        DataInterface<Long> db = createCountDataInterface("testApproximateSize");
        db.dropAllData();
        for (int i = 0; i < numOfExamples; i++) {
            db.increaseCount(Integer.toString(i));
        }
        db.flush();
        long apprSize = db.apprSize();
        Assert.assertTrue(apprSize > 100);
        Assert.assertTrue(apprSize < 10000);
        db.flush();
        apprSize = db.apprSize();
        Assert.assertTrue(apprSize > 100);
        Assert.assertTrue(apprSize < 10000);
    }

    @Test
    public void testReadValuesWithIterator() throws Exception {
        int numOfExamples = 100;
        DataInterface<Long> db = createCountDataInterface("testReadValuesWithIterator");
        db.dropAllData();
        List<Long> valuesToRead = new ArrayList<>();
        for (long i = 0; i < numOfExamples; i++) {
            db.write(i, i);
            if (i % 10 == 0) {
                valuesToRead.add(i);
            }
        }
        db.flush();
        db.read(10);
        CloseableIterator<KeyValue<Long>> valueIterator = db.iterator(valuesToRead.iterator());
        int numOfValuesRead = 0;
        Long prevKey = null;
        while (valueIterator.hasNext()) {
            KeyValue<Long> value = valueIterator.next();
            Assert.assertNotNull(value);
            Assert.assertNotNull(value.getValue());
            Assert.assertEquals(value.getKey(), value.getValue().longValue());
            numOfValuesRead++;
            Assert.assertTrue("Received " + prevKey + " before " + value.getKey(), prevKey == null || prevKey < value.getKey());
            prevKey = value.getKey();
        }
        Assert.assertEquals(valuesToRead.size(), numOfValuesRead);
        valueIterator.close();
    }

    @Test
    public void testKeyIterator() throws Exception {
        int numOfExamples = 100;
        DataInterface<Long> db = createCountDataInterface("testReadValuesWithIterator");
        db.dropAllData();
        Random random = new Random(1);
        List<Long> allKeys = new ArrayList<>();
        for (long i = 0; i < numOfExamples; i++) {
            long key = random.nextLong();
            db.write(key, key);
            allKeys.add(key);
        }
        db.flush();
        Collections.sort(allKeys);
        CloseableIterator<Long> keyIterator = db.keyIterator();
        while (keyIterator.hasNext()) {
            Long value = keyIterator.next();
            Assert.assertNotNull(value);
            allKeys.remove(value);
        }
        Assert.assertTrue(allKeys.isEmpty());
        keyIterator.close();
    }


    @Test
    public void testDataChecksumIsConsistent() throws Exception {
        int numOfExamples = 10;
        DataInterface<Long> db = createCountDataInterface("testDataChecksumIsConsistent");
        db.dropAllData();
        Random random = new Random(10);
        Set<Long> keysToIncludeInChecksum = new HashSet<>();
        for (int i = 0; i < numOfExamples; i++) {
            long key = random.nextLong();
            keysToIncludeInChecksum.add(key);
            db.write(key, random.nextLong());
        }
        db.flush();
        long checkSum = db.dataCheckSum();
        Set<Long> keysToExcludeFromCheckSum = new HashSet<>();
        for (int i = 0; i < numOfExamples; i++) {
            long key = random.nextLong();
            if (!keysToIncludeInChecksum.contains(key)) {
                keysToExcludeFromCheckSum.add(key);
                db.write(key, random.nextLong());
            }
        }
        db.flush();
        long checkSum2 = db.dataCheckSum();
        Assert.assertNotSame(checkSum, checkSum2);
        for (Long key : keysToExcludeFromCheckSum) {
            db.remove(key);
        }
        for (Long key : keysToIncludeInChecksum) {
            db.write(key, 2l);
        }
        for (Long key : keysToIncludeInChecksum) {
            db.write(key, -2l);
        }
        db.flush();
        long checkSum3 = db.dataCheckSum();
        Assert.assertEquals(checkSum, checkSum3);
        db.close();
    }

    @Test
    public void testNullString() {
        DataInterface<String> db = dataInterfaceFactory.createDataInterface(type, "testNullString", String.class, new OverWriteCombinator<String>());
        db.dropAllData();
        db.write("test", "null");
        db.flush();
        Assert.assertEquals("null", db.read("test"));
        db.write("test", null);
        db.flush();
        Assert.assertEquals(null, db.read("test"));
        db.write("test", "null");
        db.flush();
        Assert.assertEquals("null", db.read("test"));
    }

    @Test
    public void testMightContain() {
        DataInterface<String> db = dataInterfaceFactory.createDataInterface(type, "testMightContain", String.class, new OverWriteCombinator<String>());
        db.write("doescontain", "hoi");
        db.write("someothervalue1", "daag");
        db.flush();
        Assert.assertTrue(db.mightContain("doescontain"));
        Assert.assertTrue(db.mightContain("someothervalue1"));
        Assert.assertFalse(db.mightContain("someothervalue2"));
    }


    @Test
    public void testAccents() {
        DataInterface<String> dataInterface = dataInterfaceFactory.createDataInterface(type, "testAccents", String.class, new OverWriteCombinator<String>());
        String valuesWithAccents = "mé$àç7€";
        String keyWithAccents = "à§péçïàĺķĕƛ";
        dataInterface.write("key", valuesWithAccents);
        dataInterface.write(keyWithAccents, valuesWithAccents);
        dataInterface.flush();
        Assert.assertEquals(valuesWithAccents, dataInterface.read("key"));
        Assert.assertEquals(valuesWithAccents, dataInterface.read(keyWithAccents));
    }

    @Test
    public void testFlushIfNotClosed() {
        final DataInterface<Long> dataInterface = createCountDataInterface("testFlushIfNotClosed");
        dataInterface.doActionIfNotClosed(new DataInterface.ActionIfNotClosed() {
            @Override
            public void doAction() {
                dataInterface.flush();
            }
        });
        dataInterface.close();
        dataInterface.doActionIfNotClosed(new DataInterface.ActionIfNotClosed() {
            @Override
            public void doAction() {
                dataInterface.flush();
            }
        });
    }

    @Test
    public void testDataAppearsEventually() {
        DataInterface<Long> dataInterface = createCountDataInterface("testDataAppearsEventually");
        long key = 42;
        dataInterface.write(key, 10l);
        Assert.assertTrue(findValue(dataInterface, key, 10l));
        dataInterface.write(key, 1l);
        Assert.assertTrue(findValue(dataInterface, key, 11l));
    }

    private boolean findValue(DataInterface<Long> dataInterface, long key, Long targetValue) {
        long started = System.currentTimeMillis();
        boolean foundValue = false;
        while (!foundValue && System.currentTimeMillis() - started < 30000) {
            foundValue = targetValue.equals(dataInterface.read(key));
            Utils.threadSleep(10);
        }
        return foundValue;
    }

    private void writeRandomObjects(DataInterface<TestObject> dataInterface, int numOfExamples, Random random) throws Exception {
        for (int i = 0; i < numOfExamples; i++) {
            dataInterface.write(Integer.toString(random.nextInt(10000)), createRandomObject(random));
        }
        dataInterface.flush();
    }

    private TestObject createRandomObject(Random random) {
        TestObject obj = new TestObject();
        obj.setValue1(random.nextInt());
        obj.setValue2(Integer.toString(random.nextInt()));
        return obj;
    }


}
