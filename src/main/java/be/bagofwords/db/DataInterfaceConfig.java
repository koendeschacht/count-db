package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.combinator.OverWriteCombinator;
import be.bagofwords.db.experimental.index.DataIndexer;
import be.bagofwords.db.experimental.index.IndexedDataInterface;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.DataInterfaceFactoryImpl;

/**
 * Created by koen on 19/05/17.
 */
public class DataInterfaceConfig<T> {

    public final String subsetName;
    public final Class<T> objectClass;
    private final DataInterfaceFactoryImpl factory;
    public Combinator<T> combinator;
    public boolean cache;
    public boolean bloomFilter;
    public boolean isTemporary;
    public boolean inMemory;

    public DataInterfaceConfig(String subsetName, Class<T> objectClass, DataInterfaceFactoryImpl factory) {
        this.subsetName = subsetName;
        this.objectClass = objectClass;
        this.factory = factory;
        this.combinator = new OverWriteCombinator<>();
        this.cache = true;
        this.bloomFilter = false;
    }

    public DataInterfaceConfig<T> combinator(Combinator<T> combinator) {
        this.combinator = combinator;
        return this;
    }

    public DataInterfaceConfig<T> cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public DataInterfaceConfig<T> dontCache() {
        this.cache = false;
        return this;
    }

    public DataInterfaceConfig<T> bloomFilter() {
        this.bloomFilter = true;
        return this;
    }

    public DataInterfaceConfig<T> bloomFilter(boolean bloomFilter) {
        this.bloomFilter = bloomFilter;
        return this;
    }

    public DataInterfaceConfig<T> temporary() {
        this.isTemporary = true;
        return this;
    }

    public DataInterfaceConfig<T> temporary(boolean isTemporary) {
        this.isTemporary = isTemporary;
        return this;
    }

    public DataInterfaceConfig<T> inMemory() {
        this.inMemory = true;
        return this;
    }

    public BaseDataInterface<T> create() {
        return factory.createFromConfig(this);
    }

    public IndexedDataInterface<T> createIndexed(String name, DataIndexer<T> indexer) {
        return new IndexedDataInterface<>(name, factory, factory.createFromConfig(this), indexer);
    }

    public DataInterfaceConfig<T> caching(DatabaseCachingType cachingType) {
        if (!cachingType.useCache()) {
            dontCache();
        }
        if (cachingType.useBloomFilter()) {
            bloomFilter();
        }
        return this;
    }

}
