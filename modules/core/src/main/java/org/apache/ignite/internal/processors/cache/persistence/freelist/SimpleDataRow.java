package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class SimpleDataRow implements Storable {
    /** */
    private long link;

    /** */
    private final int part;

    /** */
    @GridToStringExclude
    private final byte[] val;

    public SimpleDataRow(long link, int part, byte[] val) {
        this.link = link;
        this.part = part;
        this.val = val;
    }

    public SimpleDataRow(int part, byte[] val) {
        this.part = part;
        this.val = val;
    }

    @Override public void link(long link) {
        this.link = link;
    }

    @Override public long link() {
        return link;
    }

    @Override public int partition() {
        return part;
    }

    @Override public int size() throws IgniteCheckedException {
        return 2 /** Fragment size */ + 2 /** Row size */ + value().length;
    }

    @Override public int headerSize() {
        return 0;
    }

    public byte[] value() {
        return val;
    }

    @Override public IOVersions ioVersions() {
        return SimpleDataPageIO.VERSIONS;
    }

    @Override public String toString() {
        return S.toString(SimpleDataRow.class, this, "len", val.length);
    }
}
