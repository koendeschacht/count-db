package be.bagofwords.db.data;

import be.bagofwords.db.bloomfilter.LongBloomFilter;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CompactIndex {

    public static final double FPP = 0.01;

    private int numberOfCounts;
    private long numberOfFeatures;
    private List<Long> cachedKeys;
    private LongBloomFilter filterCounts;
    private boolean wasCompacted = false;

    public CompactIndex(long numberOfFeatures, int numberOfCounts) {
        this.cachedKeys = new ArrayList<>();
        this.numberOfFeatures = numberOfFeatures;
        this.numberOfCounts = numberOfCounts;
    }

    public CompactIndex(long numberOfFeatures, LongBloomFilter filterCounts, int numberOfCounts) {
        this.numberOfFeatures = numberOfFeatures;
        this.filterCounts = filterCounts;
        this.numberOfCounts = numberOfCounts;
    }

    public CompactIndex() {

    }

    @JsonIgnore
    public boolean isSparse() {
        return filterCounts == null;
    }

    public void addKey(long key) {
        numberOfCounts++;
        if (isSparse()) {
            cachedKeys.add(key);
            wasCompacted = false;
        } else {
            filterCounts.put(key);
        }
    }

    public long getNumberOfFeatures() {
        return numberOfFeatures;
    }

    public void setNumberOfFeatures(long numberOfFeatures) {
        this.numberOfFeatures = numberOfFeatures;
    }

    public List<Long> getCachedKeys() {
        return cachedKeys;
    }

    public void setCachedKeys(List<Long> cachedKeys) {
        this.cachedKeys = cachedKeys;
    }

    public LongBloomFilter getFilterCounts() {
        return filterCounts;
    }

    public void setFilterCounts(LongBloomFilter filterCounts) {
        this.filterCounts = filterCounts;
    }

    public int getNumberOfCounts() {
        return numberOfCounts;
    }

    public void setNumberOfCounts(int numberOfCounts) {
        this.numberOfCounts = numberOfCounts;
    }

    public boolean mightContain(long key) {
        if (isSparse()) {
            return cachedKeys.contains(key);
        } else {
            return filterCounts.mightContain(key);
        }
    }

    public void makeNonSparse() {
        this.filterCounts = new LongBloomFilter(numberOfFeatures, FPP);
        for (Long key : cachedKeys) {
            filterCounts.put(key);
        }
        this.cachedKeys = null;
    }

    public CompactIndex mergeWith(CompactIndex second) {
        CompactIndex result;
        long maxSizeForSparse = this.getNumberOfFeatures() / 10;
        boolean makeSparse = this.isSparse() && second.isSparse() && this.getCachedKeys().size() + second.getCachedKeys().size() < maxSizeForSparse;
        if (makeSparse) {
            result = new CompactIndex(this.getNumberOfFeatures(), this.getNumberOfCounts() + second.getNumberOfCounts());
            result.setCachedKeys(new ArrayList<>(this.getCachedKeys()));
            for (Long key : second.getCachedKeys()) {
                result.addKey(key);
            }
        } else {
            LongBloomFilter mergedCounts;
            if (!this.isSparse() && !second.isSparse()) {
                mergedCounts = ApproximateCountsUtils.mergeBloomFilters(this.getFilterCounts(), second.getFilterCounts());
            } else {
                if (!this.isSparse()) {
                    mergedCounts = this.getFilterCounts().clone();
                } else if (!second.isSparse()) {
                    mergedCounts = second.getFilterCounts().clone();
                } else {
                    //Both sparse
                    mergedCounts = new LongBloomFilter(this.getNumberOfFeatures(), CompactIndex.FPP);
                }
            }
            if (this.isSparse()) {
                for (Long key : this.getCachedKeys()) {
                    mergedCounts.put(key);
                }
            }
            if (second.isSparse()) {
                for (Long key : second.getCachedKeys()) {
                    mergedCounts.put(key);
                }
            }
            result = new CompactIndex(this.getNumberOfFeatures(), mergedCounts, this.getNumberOfCounts() + second.getNumberOfCounts());
        }
        result.compact();
        return result;
    }

    public synchronized void compact() {
        if (!wasCompacted) {
            if (isSparse()) {
                Collections.sort(cachedKeys);
                List<Long> newCachedKeys = new ArrayList<>();
                long prev = Long.MAX_VALUE;
                for (int i = 0; i < cachedKeys.size(); i++) {
                    long curr = cachedKeys.get(i);
                    if (curr != prev) {
                        newCachedKeys.add(curr);
                    }
                    prev = curr;
                }
                cachedKeys = newCachedKeys;
            }
            wasCompacted = true;
        }
    }
}
