package be.bagofwords.db.experimental.index;

import be.bagofwords.db.BaseTestDataInterface;
import be.bagofwords.db.DatabaseBackendType;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.util.HashUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class IndexedDataInterfaceTest extends BaseTestDataInterface {

    private final Function<String, Long> tokenHasher = word -> HashUtils.hashCode(word.toLowerCase());
    private final DataIndexer<String> tokenizer = text -> Arrays.stream(text.split(" ")).map(tokenHasher).collect(toList());

    public IndexedDataInterfaceTest(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        super(type, backendType);
    }

    @Test
    public void testTextIndexer() {
        IndexedDataInterface<String> dataInterface = createInterfaceWithSomeSimpleExamples();
        List<String> results = dataInterface.readIndexedValues(tokenHasher.apply("this"));
        assertEquals(Arrays.asList("This is a test"), results);
    }

    @Test
    public void testQueryByExample() {
        IndexedDataInterface<String> dataInterface = createInterfaceWithSomeSimpleExamples();
        List<String> results = dataInterface.readIndexedValues("another test");
        assertEquals(Arrays.asList("This is a test"), results);
    }

    private IndexedDataInterface<String> createInterfaceWithSomeSimpleExamples() {
        IndexedDataInterface<String> dataInterface = dataInterfaceFactory.dataInterface("testIndexed", String.class).createIndexed("tokenIndex", tokenizer);
        dataInterface.write(1, "This is a test");
        dataInterface.write(2, "Negative example");
        dataInterface.flush();
        return dataInterface;
    }

}