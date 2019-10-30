package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.combinator.OverWriteCombinator;
import be.bagofwords.db.experimental.id.IdCombinator;
import be.bagofwords.db.experimental.id.IdDataInterface;
import be.bagofwords.db.experimental.id.IdObject;

import java.util.List;

public class IdDataInterfaceConfig<T extends IdObject> {

    public DataInterfaceConfig<List> baseConfig;

    public IdDataInterfaceConfig(DataInterfaceConfig<List> baseConfig) {
        this.baseConfig = baseConfig;
        this.baseConfig.combinator(new IdCombinator<T>(new OverWriteCombinator<>()));
    }

    public IdDataInterfaceConfig<T> cache(boolean cache) {
        this.baseConfig.cache(cache);
        return this;
    }

    public IdDataInterfaceConfig<T> combinator(Combinator<T> combinator) {
        this.baseConfig.combinator(new IdCombinator<T>(combinator));
        return this;
    }

    public IdDataInterfaceConfig<T> dontCache() {
        this.baseConfig.dontCache();
        return this;
    }

    public IdDataInterfaceConfig<T> bloomFilter() {
        this.baseConfig.bloomFilter();
        return this;
    }

    public IdDataInterfaceConfig<T> bloomFilter(boolean bloomFilter) {
        this.baseConfig.bloomFilter(bloomFilter);
        return this;
    }

    public IdDataInterfaceConfig<T> temporary() {
        this.baseConfig.temporary();
        return this;
    }

    public IdDataInterfaceConfig<T> temporary(boolean isTemporary) {
        this.baseConfig.temporary(isTemporary);
        return this;
    }

    public IdDataInterfaceConfig<T> inMemory() {
        this.baseConfig.inMemory();
        return this;
    }

    public IdDataInterfaceConfig<T> caching(DatabaseCachingType cachingType) {
        this.baseConfig.caching(cachingType);
        return this;
    }

    public IdDataInterface<T> create() {
        DataInterface<List> baseInterface = baseConfig.create();
        return new IdDataInterface<>(baseInterface);
    }

}
