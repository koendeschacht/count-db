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

@RunWith(Parameterized.class)
public class TestMultiDataInterfaceIndex extends BaseTestDataInterface {

    private final Function<String, Long> tokenHasher = word -> HashUtils.hashCode(word.toLowerCase());
    private final MultiDataIndexer<String> tokenizer = text -> Arrays.stream(text.split(" ")).map(tokenHasher).collect(toList());
    private BaseDataInterface<String> baseInterface;
    private MultiDataInterfaceIndex<String> indexedInterface;

    public TestMultiDataInterfaceIndex(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Before
    public void setup() {
        baseInterface = dataInterfaceFactory.dataInterface("testIndexed", String.class).caching(type).create();
        indexedInterface = dataInterfaceFactory.multiIndex(baseInterface, "tokens", tokenizer);
        baseInterface.write(1, "This is a test");
        baseInterface.write(2, "Negative example");
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
        List<KeyValue<String>> results = indexedInterface.read(tokenHasher.apply("this"));
        assertEquals(1, results.size());
        assertEquals("This is a test", results.get(0).getValue());
    }

    @Test
    public void testQueryByExample() {
        List<KeyValue<String>> results = indexedInterface.read("another test");
        assertEquals(1, results.size());
        assertEquals("This is a test", results.get(0).getValue());
    }

    @Test
    public void testIndexUpdates() {
        assertEquals(Collections.emptyList(), indexedInterface.read("yolo"));
        Log.i("Writing extra value");
        baseInterface.write(3, "yolo and stuff");
        baseInterface.flush();
        List<KeyValue<String>> result = indexedInterface.read("yolo");
        assertEquals(1, result.size());
        assertEquals("yolo and stuff", result.get(0).getValue());
    }

}