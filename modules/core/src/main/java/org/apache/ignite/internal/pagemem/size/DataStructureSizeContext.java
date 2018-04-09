package org.apache.ignite.internal.pagemem.size;

import java.util.Collection;

public interface DataStructureSizeContext<T, K extends DataStructureSizeContext> {
    public DataStructureSizeContext parent();

    public Collection<DataStructureSizeContext> childes();

    public K createChild(T context);

    public Collection<DataStructureSize> structures();

    public DataStructureSize sizeOf(String structure);

    public String name();
}
