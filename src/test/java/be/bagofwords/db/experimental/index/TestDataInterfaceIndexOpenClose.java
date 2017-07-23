package be.bagofwords.db.experimental.index;

import be.bagofwords.db.BaseTestDataInterface;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DatabaseBackendType;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.KeyValue;
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
    private final MultiDataIndexer<String> tokenizer = text -> Arrays.stream(text.split(" ")).map(tokenHasher).collect(toList());

    public TestDataInterfaceIndexOpenClose(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Test
    public void testOpenClose() {
        DataInterface<String> baseInterface = dataInterfaceFactory.dataInterface("testIndexed", String.class).caching(type).create();
        baseInterface.dropAllData();
        MultiDataInterfaceIndex<String> indexedInterface = dataInterfaceFactory.multiIndex(baseInterface, "tokens", tokenizer);
        baseInterface.write(1, "This is a test");
        baseInterface.write(2, "Negative example");
        baseInterface.flush();
        List<KeyValue<String>> results = indexedInterface.read("another test");
        assertEquals(1, results.size());
        assertEquals("This is a test", results.get(0).getValue());
        baseInterface.close();
        indexedInterface.close();
        if (backendType != DatabaseBackendType.MEMORY) {
            baseInterface = dataInterfaceFactory.dataInterface("testIndexed", String.class).caching(type).create();
            indexedInterface = dataInterfaceFactory.multiIndex(baseInterface, "tokens", tokenizer);
            results = indexedInterface.read("another test");
            assertEquals(1, results.size());
            assertEquals("This is a test", results.get(0).getValue());
            baseInterface.write(3, "hi there");
            baseInterface.flush();
            results = indexedInterface.read("hi");
            assertEquals(1, results.size());
            assertEquals("hi there", results.get(0).getValue());
        }
    }

}