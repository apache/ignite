/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.portables.*;
import org.gridgain.grid.kernal.processors.portable.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * {@link org.apache.ignite.IgnitePortables} implementation.
 */
public class GridPortablesImpl implements IgnitePortables {
    /** */
    private GridKernalContext ctx;

    /** */
    private GridPortableProcessor proc;

    /**
     * @param ctx Context.
     */
    public GridPortablesImpl(GridKernalContext ctx) {
        this.ctx = ctx;

        proc = ctx.portable();
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        guard();

        try {
            return proc.typeId(typeName);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T toPortable(@Nullable Object obj) throws PortableException {
        guard();

        try {
            return (T)proc.marshalToPortable(obj);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public PortableBuilder builder(int typeId) {
        guard();

        try {
            return proc.builder(typeId);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public PortableBuilder builder(String typeName) {
        guard();

        try {
            return proc.builder(typeName);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public PortableBuilder builder(GridPortableObject portableObj) {
        guard();

        try {
            return proc.builder(portableObj);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridPortableMetadata metadata(Class<?> cls) throws PortableException {
        guard();

        try {
            return proc.metadata(proc.typeId(cls.getName()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridPortableMetadata metadata(String typeName) throws PortableException {
        guard();

        try {
            return proc.metadata(proc.typeId(typeName));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridPortableMetadata metadata(int typeId) throws PortableException {
        guard();

        try {
            return proc.metadata(typeId);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridPortableMetadata> metadata() throws PortableException {
        guard();

        try {
            return proc.metadata();
        }
        finally {
            unguard();
        }
    }

    /**
     * @return Portable processor.
     */
    public GridPortableProcessor processor() {
        return proc;
    }

    /**
     * <tt>ctx.gateway().readLock()</tt>
     */
    private void guard() {
        ctx.gateway().readLock();
    }

    /**
     * <tt>ctx.gateway().readUnlock()</tt>
     */
    private void unguard() {
        ctx.gateway().readUnlock();
    }
}
