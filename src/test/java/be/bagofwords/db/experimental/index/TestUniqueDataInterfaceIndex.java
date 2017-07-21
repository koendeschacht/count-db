package be.bagofwords.db.experimental.index;

import be.bagofwords.db.BaseTestDataInterface;
import be.bagofwords.db.DatabaseBackendType;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.logging.Log;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestUniqueDataInterfaceIndex extends BaseTestDataInterface {

    private final UniqueDataIndexer<Long> index = value -> {
        if (value.equals(10L)) {
            return 10L; //Trigger exception when writing both values 5 and 10
        } else {
            return value * 2;
        }
    };
    private BaseDataInterface<Long> baseInterface;
    private UniqueDataInterfaceIndex<Long> indexedInterface;

    public TestUniqueDataInterfaceIndex(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Before
    public void setup() {
        baseInterface = dataInterfaceFactory.dataInterface("testIndexed", Long.class).caching(type).create();
        indexedInterface = dataInterfaceFactory.uniqueIndex(baseInterface, "tokens", index);
        baseInterface.write(1, 1L);
        baseInterface.write(2, 2L);
        baseInterface.flush();
    }

    @After
    public void tearDown() {
        baseInterface.dropAllData();
        baseInterface.close();
        // indexedInterface.close();
    }

    @Test
    public void testTextIndexer() {
        KeyValue<Long> result = indexedInterface.read(2);
        assertEquals(new KeyValue<>(1, 1L), result);
    }

    @Test
    public void testQueryByExample() {
        KeyValue<Long> result = indexedInterface.read(new Long(1)); //Query by example
        assertEquals(new KeyValue<>(1, 1L), result);
    }

    @Test
    public void testIndexUpdates() {
        assertEquals(null, indexedInterface.read(6));
        baseInterface.write(3, 3L);
        baseInterface.flush();
        assertEquals(new KeyValue<>(3, 3L), indexedInterface.read(6));
    }

    @Test
    public void testExceptionWhenWriting() {
        baseInterface.write(5, 5L);
        try {
            baseInterface.write(10, 10L);
            baseInterface.flush();
            indexedInterface.read(5L);
            fail();
        } catch (Exception exp) {
            //OK
        }
    }

}