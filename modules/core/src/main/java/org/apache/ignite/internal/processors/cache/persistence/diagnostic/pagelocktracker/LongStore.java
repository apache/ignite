package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

public interface LongStore {

    int capacity();

    long getByIndex(int idx);

    void setByIndex(int idx, long val);

    long[] copy();

    void free();
}
