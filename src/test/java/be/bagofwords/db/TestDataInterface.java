package be.bagofwords.db;

import be.bagofwords.db.helper.EvenKeysFilter;
import be.bagofwords.db.helper.EvenNumbersValueFilter;
import be.bagofwords.db.helper.TestObject;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.methods.RangeKeyFilter;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.Utils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(Parameterized.class)
public class TestDataInterface extends BaseTestDataInterface {

    public TestDataInterface(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Test
    public void sanityCheck() throws Exception {
        Random random = new Random(1204);
        BaseDataInterface<TestObject> dataInterface = dataInterfaceFactory.dataInterface("sanityCheck", TestObject.class).caching(type).create();
        dataInterface.dropAllData();
        writeRandomObjects(dataInterface, 200, random);
        TestObject randomObj = createRandomObject(random);
        dataInterface.write("obj", randomObj);
        dataInterface.flush();
        writeRandomObjects(dataInterface, 200, random);
        TestObject readObj = dataInterface.read("obj");
        Assert.assertEquals(randomObj, readObj);
    }

    @Test
    public void testDropData() throws Exception {
        Random random = new Random();
        TestObject randomObj = createRandomObject(random);
        BaseDataInterface<TestObject> dataInterface = dataInterfaceFactory.dataInterface("testDropData", TestObject.class).caching(type).create();
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
        BaseDataInterface<Integer> dataInterface = dataInterfaceFactory.dataInterface("testIterator", Integer.class).caching(type).create();
        dataInterface.dropAllData();
        for (int i = 0; i < numOfExamples; i++) {
            dataInterface.write(i, i);
        }
        dataInterface.flush();
        assertIteratorReturnsCorrectValues(numOfExamples, dataInterface);
        assertIteratorReturnsCorrectValues(numOfExamples, dataInterface); //we test this twice because it has happened that a second invocation gave a different iterator
    }

    private void assertIteratorReturnsCorrectValues(int numOfExamples, DataInterface<Integer> dataInterface) {
        CloseableIterator<KeyValue<Integer>> it = dataInterface.iterator();
        int numOfValuesInIterator = 0;
        while (it.hasNext()) {
            KeyValue<Integer> next = it.next();
            long key = next.getKey();
            Integer value = next.getValue();
            int expectedValue = (int) key;
            Assert.assertEquals(value.intValue(), expectedValue);
            numOfValuesInIterator++;
        }
        Assert.assertEquals(numOfExamples, numOfValuesInIterator);
        it.close();
    }

    @Test
    public void testRandomValues() throws Exception {
        long numOfExamples = 200;
        BaseDataInterface<TestObject> dataInterface = dataInterfaceFactory.dataInterface("testRandomValues", TestObject.class).caching(type).create();
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
        long numOfExamples = 100;
        DataInterface<Long> dataInterface = createCountDataInterface("testCountsWithPause");
        dataInterface.dropAllData();
        for (int i = 0; i < numOfExamples; i++) {
            dataInterface.write((long) i, 2l * i);
        }
        dataInterface.flush();
        Utils.threadSleep(1000); //Give the cleanup threads time to run
        for (int i = 0; i < numOfExamples; i += 73) {
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
            db.increaseCount(i);
        }
        for (int i = 0; i < numOfExamples; i += 2) {
            db.write(i, null);
        }
        db.flush();
        for (int i = 0; i < numOfExamples; i++) {
            Long count = db.readCount(i);
            if (i % 2 == 0) {
                Assert.assertEquals(i + " should be 0", 0, count.intValue());
            } else {
                Assert.assertEquals(i + " should be 1", 1, count.intValue());
            }
        }
    }

    @Test
    public void testApproximateSize() throws Exception {
        int numOfExamples = 1000;
        DataInterface<Long> db = createCountDataInterface("testApproximateSize");
        db.dropAllData();
        db.write(IntStream.range(0, numOfExamples).mapToObj(i -> new KeyValue<>(HashUtils.hashCode(Integer.toString(i)), 1l)).iterator());
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
        CloseableIterator<KeyValue<Long>> valueIterator = db.iterator(IterableUtils.iterator(valuesToRead));
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
        long checkSum = db.apprDataChecksum();
        Set<Long> keysToExcludeFromCheckSum = new HashSet<>();
        for (int i = 0; i < numOfExamples; i++) {
            long key = random.nextLong();
            if (!keysToIncludeInChecksum.contains(key)) {
                keysToExcludeFromCheckSum.add(key);
                db.write(key, random.nextLong());
            }
        }
        db.flush();
        long checkSum2 = db.apprDataChecksum();
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
        long checkSum3 = db.apprDataChecksum();
        Assert.assertEquals(checkSum, checkSum3);
        db.close();
    }

    @Test
    public void testNullString() {
        BaseDataInterface<String> db = dataInterfaceFactory.dataInterface("testNullString", String.class).caching(type).create();
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
        BaseDataInterface<String> db = dataInterfaceFactory.dataInterface("testMightContain", String.class).caching(type).create();
        db.write("doescontain", "hoi");
        db.write("someothervalue1", "daag");
        db.flush();
        Assert.assertTrue(db.mightContain("doescontain"));
        Assert.assertTrue(db.mightContain("someothervalue1"));
        Assert.assertFalse(db.mightContain("someothervalue2"));
    }

    @Test
    public void testAccents() {
        BaseDataInterface<String> dataInterface = dataInterfaceFactory.dataInterface("testAccents", String.class).caching(type).create();
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
        dataInterface.ifNotClosed(dataInterface::flush);
        dataInterface.close();
        dataInterface.ifNotClosed(dataInterface::flush);
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

    @Test
    public void testIteratorWithFilter() {
        DataInterface<Long> dataInterface = createCountDataInterface("testIteratorWithFilter");
        int numOfItems = 100;
        for (int i = 0; i < 100; i++) {
            dataInterface.write(i, (long) i);
        }
        dataInterface.flush();
        //Try with stream
        MutableInt numOfValuesRead = new MutableInt();
        dataInterface.stream(new EvenKeysFilter()).forEach((v) -> numOfValuesRead.increment());
        Assert.assertEquals(numOfItems / 2, numOfValuesRead.intValue());
        //Try with iterator
        numOfValuesRead.setValue(0);
        CloseableIterator<KeyValue<Long>> closeableIterator = dataInterface.iterator(new EvenKeysFilter());
        while (closeableIterator.hasNext()) {
            closeableIterator.next();
            numOfValuesRead.increment();
        }
        closeableIterator.close();
        Assert.assertEquals(numOfItems / 2, numOfValuesRead.intValue());
    }

    @Test
    public void testValuesIteratorWithFilter() {
        DataInterface<Long> dataInterface = createCountDataInterface("testValuesIteratorWithFilter");
        int numOfItems = 100;
        for (int i = 0; i < 100; i++) {
            dataInterface.write(i, (long) i);
        }
        dataInterface.flush();
        //Try with stream
        MutableInt numOfValuesRead = new MutableInt();
        dataInterface.streamValues(new EvenKeysFilter()).forEach((v) -> numOfValuesRead.increment());
        Assert.assertEquals(numOfItems / 2, numOfValuesRead.intValue());
        //Try with iterator
        numOfValuesRead.setValue(0);
        CloseableIterator<Long> closeableIterator = dataInterface.valueIterator(new EvenKeysFilter());
        while (closeableIterator.hasNext()) {
            closeableIterator.next();
            numOfValuesRead.increment();
        }
        closeableIterator.close();
        Assert.assertEquals(numOfItems / 2, numOfValuesRead.intValue());
    }

    @Test
    public void testValuesIteratorWithRangeFilter() {
        DataInterface<Long> dataInterface = createCountDataInterface("testValuesIteratorWithRangeFilter");
        int numOfItems = 100;
        for (int i = 0; i < numOfItems; i++) {
            dataInterface.write(i, (long) i);
        }
        dataInterface.flush();
        //Try with stream
        MutableInt numOfValuesRead = new MutableInt();
        dataInterface.streamValues(new RangeKeyFilter(0, 50)).forEach((v) -> numOfValuesRead.increment());
        Assert.assertEquals(numOfItems / 2, numOfValuesRead.intValue());
        //Try with iterator
        numOfValuesRead.setValue(0);
        CloseableIterator<Long> closeableIterator = dataInterface.valueIterator(new RangeKeyFilter(0, 50));
        while (closeableIterator.hasNext()) {
            closeableIterator.next();
            numOfValuesRead.increment();
        }
        closeableIterator.close();
        Assert.assertEquals(numOfItems / 2, numOfValuesRead.intValue());
    }

    @Test
    public void testStreamValuesWithValueFilter() {
        DataInterface<Long> dataInterface = createCountDataInterface("testValuesIteratorWithValuesFilter");
        int numOfItems = 100;
        for (int i = 0; i < numOfItems; i++) {
            dataInterface.write(i, (long) i);
        }
        dataInterface.flush();
        Set<Long> filteredValues = dataInterface.streamValues(new EvenNumbersValueFilter()).collect(Collectors.toSet());
        Assert.assertEquals(numOfItems / 2, filteredValues.size());
        for (long i = 0; i < numOfItems; i += 2) {
            Assert.assertTrue(filteredValues.contains(i));
        }
    }

    @Test
    public void testIteratorWithKeyIterator() {
        DataInterface<Long> dataInterface = createCountDataInterface("testIteratorWithKeyIterator");
        int numOfItems = 100;
        List<Long> keysToRequest = new ArrayList<>();
        for (int i = 0; i < numOfItems; i++) {
            dataInterface.write(i, (long) i);
            if (i % 3 == 0) {
                keysToRequest.add((long) i);
            }
        }
        dataInterface.flush();
        //Read some values, so that they are cached (only influences test if caching is used)
        for (int i = 0; i < numOfItems; i += 6) {
            dataInterface.read(i);
        }
        List<Long> readValues = dataInterface.streamValues(IterableUtils.iterator(keysToRequest)).collect(Collectors.toList());
        Assert.assertEquals((int) Math.ceil(numOfItems / 3.0), readValues.size());
        for (int i = 0; i < numOfItems; i += 3) {
            Assert.assertEquals(new Long(i), readValues.get(i / 3));
        }
    }

    private boolean findValue(DataInterface<Long> dataInterface, long key, Long targetValue) {
        long started = System.currentTimeMillis();
        boolean foundValue = false;
        while (!foundValue && System.currentTimeMillis() - started < 2000) {
            foundValue = targetValue.equals(dataInterface.read(key));
            Utils.threadSleep(10);
        }
        dataInterface.flush();
        while (!foundValue && System.currentTimeMillis() - started < 10000) {
            foundValue = targetValue.equals(dataInterface.read(key));
            Utils.threadSleep(10);
        }
        return foundValue;
    }

    private void writeRandomObjects(BaseDataInterface<TestObject> dataInterface, int numOfExamples, Random random) throws Exception {
        dataInterface.write(IntStream.range(0, numOfExamples)
                .mapToObj(i -> new KeyValue<>(HashUtils.hashCode(Integer.toString(random.nextInt(10000))), createRandomObject(random)))
                .iterator());
        dataInterface.flush();
    }

    private TestObject createRandomObject(Random random) {
        TestObject obj = new TestObject();
        obj.setValue1(random.nextInt());
        obj.setValue2(Integer.toString(random.nextInt()));
        return obj;
    }

}
