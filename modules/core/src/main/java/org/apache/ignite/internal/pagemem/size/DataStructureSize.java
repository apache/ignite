package org.apache.ignite.internal.pagemem.size;

public interface DataStructureSize {

    public void inc();

    public void dec();

    public void add(long val);

    public long size();

    public String name();
}
