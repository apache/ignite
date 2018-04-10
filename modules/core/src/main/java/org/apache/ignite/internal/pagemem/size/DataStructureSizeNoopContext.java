package org.apache.ignite.internal.pagemem.size;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.pagemem.wal.DataStructureSizeAdapter;

public class DataStructureSizeNoopContext implements DataStructureSizeContext {
    @Override public DataStructureSizeContext parent() {
        return this;
    }

    @Override public Collection<DataStructureSizeContext> childes() {
        return Collections.emptyList();
    }

    @Override public DataStructureSizeContext createChild(Object context) {
        return this;
    }

    @Override public Collection<DataStructureSize> structures() {
        return Collections.emptyList();
    }

    @Override public DataStructureSize sizeOf(String structure) {
        return new DataStructureSizeAdapter() { };
    }

    @Override public String name() {
        return "NOOP";
    }
}
