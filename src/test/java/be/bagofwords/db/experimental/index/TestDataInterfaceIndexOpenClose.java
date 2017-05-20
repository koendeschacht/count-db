package be.bagofwords.db.experimental.index;

import be.bagofwords.db.BaseTestDataInterface;
import be.bagofwords.db.DatabaseBackendType;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.logging.Log;
import be.bagofwords.util.HashUtils;
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
public class TestDataInterfaceIndexOpenClose extends BaseTestDataInterface {

    private final Function<String, Long> tokenHasher = word -> HashUtils.hashCode(word.toLowerCase());
    private final DataIndexer<String> tokenizer = text -> Arrays.stream(text.split(" ")).map(tokenHasher).collect(toList());
    private BaseDataInterface<String> baseInterface;
    private DataInterfaceIndex<String> indexedInterface;

    public TestDataInterfaceIndexOpenClose(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Before
    public void setup() {
        baseInterface = dataInterfaceFactory.dataInterface("testIndexed", String.class).caching(type).create();
        indexedInterface = dataInterfaceFactory.index(baseInterface, "tokens", tokenizer);
        baseInterface.write(1, "This is a test");
        baseInterface.write(2, "Negative example");
        baseInterface.flush();
    }

    @After
    public void tearDown() {
        baseInterface.dropAllData();
        baseInterface.close();
        indexedInterface.close();
    }

    @Test
    public void testTextIndexer() {
        List<String> results = indexedInterface.readIndexedValues(tokenHasher.apply("this"));
        assertEquals(Collections.singletonList("This is a test"), results);
    }

    @Test
    public void testQueryByExample() {
        List<String> results = indexedInterface.readIndexedValues("another test");
        assertEquals(Collections.singletonList("This is a test"), results);
    }

    @Test
    public void testIndexUpdates() {
        assertEquals(Collections.emptyList(), indexedInterface.readIndexedValues("yolo"));
        Log.i("Writing extra value");
        baseInterface.write(3, "yolo and stuff");
        baseInterface.flush();
        assertEquals(Collections.singletonList("yolo and stuff"), indexedInterface.readIndexedValues("yolo"));
    }

}