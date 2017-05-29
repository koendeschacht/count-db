package be.bagofwords.db;

import be.bagofwords.db.combinator.DoubleCombinator;
import be.bagofwords.db.combinator.FloatCombinator;
import be.bagofwords.db.combinator.IntegerCombinator;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.impl.BaseDataInterface;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/19/14.
 */

@RunWith(Parameterized.class)
public class TestDataInterfacePrimitiveValues extends BaseTestDataInterface {

    public TestDataInterfacePrimitiveValues(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Test
    public void testLongValues() {
        final DataInterface<Long> db = dataInterfaceFactory.dataInterface("testLongValues", Long.class).combinator(new LongCombinator()).caching(type).create();
        db.dropAllData();
        for (int i = 0; i < 100; i++) {
            db.write(i, (long) i);
        }
        db.write(0, null);
        db.flush();
        Assert.assertNull(db.read(0));
        for (int i = 1; i < 100; i++) {
            Long value = db.read(i);
            Assert.assertEquals(new Long(i), value);
        }
    }

    @Test
    public void testDoubleValues() {
        final BaseDataInterface<Double> db = dataInterfaceFactory.dataInterface("testDoubleValues", Double.class).combinator(new DoubleCombinator()).caching(type).create();
        db.dropAllData();
        for (int i = 0; i < 1000; i++) {
            db.write(i, (double) i);
        }
        db.write(0, null);
        db.flush();
        Assert.assertNull(db.read(0));
        for (int i = 1; i < 1000; i++) {
            Double value = db.read(i);
            Assert.assertEquals(new Double(i), value);
        }
    }

    @Test
    public void testIntegerValues() {
        final BaseDataInterface<Integer> db = dataInterfaceFactory.dataInterface("testIntegerValues", Integer.class).combinator(new IntegerCombinator()).caching(type).create();
        db.dropAllData();
        for (int i = 0; i < 1000; i++) {
            db.write(i, i);
        }
        db.write(0, null);
        db.flush();
        Assert.assertNull(db.read(0));
        for (int i = 1; i < 1000; i++) {
            Integer value = db.read(i);
            Assert.assertEquals(new Integer(i), value);
        }
    }

    @Test
    public void testFloatValues() {
        final BaseDataInterface<Float> db = dataInterfaceFactory.dataInterface("testFloatValues", Float.class).combinator(new FloatCombinator()).caching(type).create();
        db.dropAllData();
        for (int i = 0; i < 1000; i++) {
            db.write(i, (float) i);
        }
        db.write(0, null);
        db.flush();
        Assert.assertNull(db.read(0));
        for (int i = 1; i < 1000; i++) {
            Float value = db.read(i);
            Assert.assertEquals(new Float(i), value);
        }
    }
}
