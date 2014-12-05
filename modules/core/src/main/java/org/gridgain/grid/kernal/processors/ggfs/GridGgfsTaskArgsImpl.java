/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * GGFS task arguments implementation.
 */
public class GridGgfsTaskArgsImpl<T> implements GridGgfsTaskArgs<T>,  Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** GGFS name. */
    private String ggfsName;

    /** Paths. */
    private Collection<IgniteFsPath> paths;

    /** Record resolver. */
    private IgniteFsRecordResolver recRslvr;

    /** Skip non existent files flag. */
    private boolean skipNonExistentFiles;

    /** Maximum range length. */
    private long maxRangeLen;

    /** User argument. */
    private T usrArg;

    /**
     * {@link Externalizable} support.
     */
    public GridGgfsTaskArgsImpl() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param ggfsName GGFS name.
     * @param paths Paths.
     * @param recRslvr Record resolver.
     * @param skipNonExistentFiles Skip non existent files flag.
     * @param maxRangeLen Maximum range length.
     * @param usrArg User argument.
     */
    public GridGgfsTaskArgsImpl(String ggfsName, Collection<IgniteFsPath> paths, IgniteFsRecordResolver recRslvr,
        boolean skipNonExistentFiles, long maxRangeLen, T usrArg) {
        this.ggfsName = ggfsName;
        this.paths = paths;
        this.recRslvr = recRslvr;
        this.skipNonExistentFiles = skipNonExistentFiles;
        this.maxRangeLen = maxRangeLen;
        this.usrArg = usrArg;
    }

    /** {@inheritDoc} */
    @Override public String ggfsName() {
        return ggfsName;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsPath> paths() {
        return paths;
    }

    /** {@inheritDoc} */
    @Override public IgniteFsRecordResolver recordResolver() {
        return recRslvr;
    }

    /** {@inheritDoc} */
    @Override public boolean skipNonExistentFiles() {
        return skipNonExistentFiles;
    }

    /** {@inheritDoc} */
    @Override public long maxRangeLength() {
        return maxRangeLen;
    }

    /** {@inheritDoc} */
    @Override public T userArgument() {
        return usrArg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsTaskArgsImpl.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, ggfsName);
        U.writeCollection(out, paths);

        out.writeObject(recRslvr);
        out.writeBoolean(skipNonExistentFiles);
        out.writeLong(maxRangeLen);
        out.writeObject(usrArg);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ggfsName = U.readString(in);
        paths = U.readCollection(in);

        recRslvr = (IgniteFsRecordResolver)in.readObject();
        skipNonExistentFiles = in.readBoolean();
        maxRangeLen = in.readLong();
        usrArg = (T)in.readObject();
    }
}
