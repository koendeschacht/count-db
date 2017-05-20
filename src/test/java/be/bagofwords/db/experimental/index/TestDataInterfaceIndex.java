package be.bagofwords.db.experimental.index;

import be.bagofwords.db.BaseTestDataInterface;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DatabaseBackendType;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.logging.Log;
import be.bagofwords.util.HashUtils;
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
public class TestDataInterfaceIndex extends BaseTestDataInterface {

    private final Function<String, Long> tokenHasher = word -> HashUtils.hashCode(word.toLowerCase());
    private final DataIndexer<String> tokenizer = text -> Arrays.stream(text.split(" ")).map(tokenHasher).collect(toList());

    public TestDataInterfaceIndex(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Test
    public void testOpenClose() {
        DataInterface<String> baseInterface = dataInterfaceFactory.dataInterface("testIndexed", String.class).caching(type).create();
        DataInterfaceIndex<String> indexedInterface = dataInterfaceFactory.index(baseInterface, "tokens", tokenizer);
        baseInterface.write(1, "This is a test");
        baseInterface.write(2, "Negative example");
        baseInterface.flush();
        List<String> results = indexedInterface.readIndexedValues("another test");
        assertEquals(Collections.singletonList("This is a test"), results);
        baseInterface.close();
        indexedInterface.close();
        baseInterface = dataInterfaceFactory.dataInterface("testIndexed", String.class).caching(type).create();
        indexedInterface = dataInterfaceFactory.index(baseInterface, "tokens", tokenizer);
        results = indexedInterface.readIndexedValues("another test");
        assertEquals(Collections.singletonList("This is a test"), results);
        Log.i("Writing new value");
        baseInterface.write(3, "hi there");
        baseInterface.flush();
        results = indexedInterface.readIndexedValues("hi");
        assertEquals(Collections.singletonList("hi there"), results);
    }

}