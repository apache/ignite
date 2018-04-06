package org.apache.ignite.internal.pagemem;

import java.util.Collection;

public interface DataStructureSizeNode<T, K extends DataStructureSizeNode> {
    public DataStructureSizeNode parent();

    public Collection<DataStructureSizeNode> childes();

    public K createChild(T context);

    public Collection<DataStructureSize> structures();

    public DataStructureSize sizeOf(String name);

    public String name();
}
