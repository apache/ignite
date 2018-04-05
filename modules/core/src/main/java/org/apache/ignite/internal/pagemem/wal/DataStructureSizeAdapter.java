package org.apache.ignite.internal.pagemem.wal;

import org.apache.ignite.internal.pagemem.DataStructureSize;

public abstract class DataStructureSizeAdapter implements DataStructureSize {
    @Override public void inc() {

    }

    @Override public void dec() {

    }

    @Override public void add(long val) {

    }

    @Override public long size() {
        return 0;
    }

    @Override public String name() {
        return null;
    }
}
