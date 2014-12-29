/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.processor.*;
import java.io.*;

/**
 * Implementation of {@link EntryProcessorResult}.
 */
public class CacheInvokeResult<T> implements EntryProcessorResult<T>, Externalizable, IgniteOptimizedMarshallable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "JavaAbbreviationUsage", "UnusedDeclaration"})
    private static Object GG_CLASS_ID;

    /** */
    @GridToStringInclude
    private T res;

    /** */
    private Exception err;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public CacheInvokeResult() {
        // No-op.
    }

    /**
     * @param res Computed result.
     */
    public CacheInvokeResult(T res) {
        assert res != null;

        this.res = res;
    }

    /**
     * @param err Exception thrown by {@link EntryProcessor#process(MutableEntry, Object...)}.
     */
    public CacheInvokeResult(Exception err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public Object ggClassId() {
        return GG_CLASS_ID;
    }

    /** {@inheritDoc} */
    @Override public T get() throws EntryProcessorException {
        if (err != null) {
            if (err instanceof EntryProcessorException)
                throw (EntryProcessorException)err;

            throw new EntryProcessorException(err);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(res);

        out.writeObject(err);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        res = (T)in.readObject();

        err = (Exception)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheInvokeResult.class, this);
    }
}
