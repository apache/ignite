package org.apache.ignite.internal.pagemem;

public interface DataStructureSize {

    public void inc();

    public void dec();

    public long size();

    public String name();
}
