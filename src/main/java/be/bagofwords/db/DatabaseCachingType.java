package be.bagofwords.db;

/**
 * Specify whether you want the data interface cached, and if you want to add a bloom filter
 */

public enum DatabaseCachingType {

    DIRECT(false, false), CACHED(true, false), CACHED_AND_BLOOM(true, true), BLOOM(false, true);

    private boolean useCache;
    private boolean useBloomFilter;

    DatabaseCachingType(boolean useCache, boolean useBloomFilter) {
        this.useCache = useCache;
        this.useBloomFilter = useBloomFilter;
    }

    public boolean useCache() {
        return useCache;
    }

    public boolean useBloomFilter() {
        return useBloomFilter;
    }
}
