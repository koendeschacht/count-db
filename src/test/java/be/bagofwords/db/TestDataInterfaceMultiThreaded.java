package be.bagofwords.db;

import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.SafeThread;
import be.bagofwords.util.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

@RunWith(Parameterized.class)
public class TestDataInterfaceMultiThreaded extends BaseTestDataInterface {

    public TestDataInterfaceMultiThreaded(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Test
    public void testCountsWriteDelete() throws InterruptedException {
        final int numOfThreads = 10;
        final int numOfExamples = 200;
        final int numOfIterations = 10000;
        String name = "testMultiThreaded_" + type;
        final DataInterface<Long> db = dataInterfaceFactory.dataInterface(name, Long.class).combinator(new LongCombinator()).caching(type).create();
        db.dropAllData();

        final long[] numAdded = new long[numOfExamples];
        final CountDownLatch countDownLatch = new CountDownLatch(numOfThreads);
        //Add multithreaded
        for (int i = 0; i < numOfThreads; i++) {
            Thread t = new Thread("testStringsThread") {

                @Override
                public void run() {
                    Random r = new Random();
                    for (int i = 0; i < numOfIterations; i++) {
                        int nextInt = r.nextInt(numOfExamples);
                        synchronized (numAdded) {
                            numAdded[nextInt]++;
                        }
                        db.increaseCount(Integer.toString(nextInt), 1l);
                    }
                    countDownLatch.countDown();
                }
            };
            t.start();
        }
        countDownLatch.await();
        db.flush();
        for (int i = 0; i < numOfExamples; i++) {
            Assert.assertEquals("Not equal on position " + i, numAdded[i], db.readCount(Integer.toString(i)));
        }
        //Delete multithreaded:
        for (int i = 0; i < numOfThreads; i++) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    Random r = new Random();
                    while (!allRemoved(numAdded)) {
                        int pos = r.nextInt(numAdded.length);
                        if (numAdded[pos] > 0) {
                            db.write(Integer.toString(pos), null);
                            numAdded[pos] = 0;
                        }
                    }
                }
            };
            t.start();
        }
        while (!allRemoved(numAdded)) {
            Utils.threadSleep(100);
        }
        db.flush();
        for (int i = 0; i < numOfExamples; i++) {
            Assert.assertEquals(0, db.readCount(Integer.toString(i)));
        }
    }

    @Test
    public void testCounts() {
        int numOfThreads = 10;
        final int numOfWritesPerThread = 50;
        final DataInterface<Long> db = createCountDataInterface("testCounts");
        db.dropAllData();
        SafeThread[] threads = new SafeThread[numOfThreads];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new SafeThread("testThread_" + i, false) {
                @Override
                protected void runImpl() throws Exception {
                    for (int i = 0; i < numOfWritesPerThread; i++) {
                        for (int j = 0; j < 1000; j++) {
                            db.increaseCount(HashUtils.randomDistributeHash(j));
                        }
                    }
                }
            };
            threads[i].start();
        }
        SafeThread flushThread = new SafeThread("flushThread", false) {
            @Override
            protected void runImpl() throws Exception {
                for (int j = 0; j < 50; j++) {
                    db.flush();
                    Utils.threadSleep(100);
                }
            }
        };
        flushThread.start();
        for (SafeThread thread : threads) {
            thread.waitForFinish();
        }
        flushThread.waitForFinish();
        db.flush();
        for (int j = 0; j < 1000; j++) {
            db.readCount(HashUtils.randomDistributeHash(j));
        }
        CloseableIterator<KeyValue<Long>> iterator = db.iterator();
        while (iterator.hasNext()) {
            KeyValue<Long> curr = iterator.next();
            Assert.assertEquals("Incorrect total for " + curr.getKey() + " " + curr.getValue(), numOfThreads * numOfWritesPerThread, curr.getValue().longValue());
        }
        iterator.close();
    }

    /**
     * Test that writes sufficient data to trigger a rewrite of the files in FileDataInterface
     */
    @Test
    public void testLargeStrings() {
        int numOfThreads = 10;
        final int numOfWritesPerThread = 100;
        final DataInterface<String> db = createDataInterface("testStrings_" + type, String.class).create();
        final String longValue = longValue();
        db.dropAllData();
        SafeThread[] threads = new SafeThread[numOfThreads];
        for (int i = 0; i < threads.length; i++) {
            int threadNr = i;
            threads[i] = new SafeThread("testThread_" + i, false) {
                @Override
                protected void runImpl() {
                    for (int j = 0; j < numOfWritesPerThread; j++) {
                        db.write(HashUtils.randomDistributeHash(threadNr) + "_" + HashUtils.randomDistributeHash(j), longValue);
                        if (j % 20 == 0) {
                            db.flush();
                        }
                    }
                }

            };
            threads[i].start();
        }
        for (SafeThread thread : threads) {
            thread.waitForFinish();
        }
        db.flush();
        for (int i = 0; i < numOfThreads; i++) {
            for (int j = 0; j < numOfWritesPerThread; j++) {
                String value = db.read(HashUtils.randomDistributeHash(i) + "_" + HashUtils.randomDistributeHash(j));
                Assert.assertEquals(longValue, value);
            }
        }
    }

    private String longValue() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < 20000; i++) {
            result.append(HashUtils.randomDistributeHash(i));
        }
        return result.toString();
    }

    private boolean allRemoved(long[] numAdded) {
        boolean allRemoved = true;
        for (long aNumAdded : numAdded) {
            allRemoved &= aNumAdded == 0;
        }
        return allRemoved;
    }

}
