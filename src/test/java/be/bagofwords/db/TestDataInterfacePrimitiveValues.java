package be.bagofwords.db;

import be.bagofwords.db.combinator.DoubleCombinator;
import be.bagofwords.db.combinator.FloatCombinator;
import be.bagofwords.db.combinator.IntegerCombinator;
import be.bagofwords.db.combinator.LongCombinator;
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
        final DataInterface<Long> db = dataInterfaceFactory.createDataInterface(type, "testLongValues", Long.class, new LongCombinator());
        db.dropAllData();
        for (int i = 0; i < 1000; i++) {
            db.write(i, (long) i);
        }
        db.write(0, null);
        db.flush();
        Assert.assertNull(db.read(0));
        for (int i = 1; i < 1000; i++) {
            Long value = db.read(i);
            Assert.assertEquals(new Long(i), value);
        }
    }

    @Test
    public void testDoubleValues() {
        final DataInterface<Double> db = dataInterfaceFactory.createDataInterface(type, "testDoubleValues", Double.class, new DoubleCombinator());
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
        final DataInterface<Integer> db = dataInterfaceFactory.createDataInterface(type, "testIntegerValues", Integer.class, new IntegerCombinator());
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
        final DataInterface<Float> db = dataInterfaceFactory.createDataInterface(type, "testFloatValues", Float.class, new FloatCombinator());
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
