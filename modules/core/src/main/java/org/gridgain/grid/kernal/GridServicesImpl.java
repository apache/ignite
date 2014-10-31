/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.design.lang.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * {@link GridCompute} implementation.
 */
public class GridServicesImpl extends IgniteAsyncSupportAdapter implements GridServices, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridKernalContext ctx;

    /** */
    private GridProjectionAdapter prj;

    /**
     * Required by {@link Externalizable}.
     */
    public GridServicesImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     */
    public GridServicesImpl(GridKernalContext ctx, GridProjectionAdapter prj) {
        this.ctx = ctx;
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public GridProjection projection() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public void deployNodeSingleton(String name, GridService svc) throws GridException {
        A.notNull(name, "name");
        A.notNull(svc, "svc");

        guard();

        try {
            saveOrGet(ctx.service().deployNodeSingleton(prj, name, svc));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void deployClusterSingleton(String name, GridService svc) throws GridException {
        A.notNull(name, "name");
        A.notNull(svc, "svc");

        guard();

        try {
            saveOrGet(ctx.service().deployClusterSingleton(prj, name, svc));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void deployMultiple(String name, GridService svc, int totalCnt, int maxPerNodeCnt)
        throws GridException {
        A.notNull(name, "name");
        A.notNull(svc, "svc");

        guard();

        try {
            saveOrGet(ctx.service().deployMultiple(prj, name, svc, totalCnt, maxPerNodeCnt));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void deployKeyAffinitySingleton(String name, GridService svc, @Nullable String cacheName,
        Object affKey) throws GridException {
        A.notNull(name, "name");
        A.notNull(svc, "svc");
        A.notNull(affKey, "affKey");

        guard();

        try {
            saveOrGet(ctx.service().deployKeyAffinitySingleton(name, svc, cacheName, affKey));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void deploy(GridServiceConfiguration cfg) throws GridException {
        A.notNull(cfg, "cfg");

        guard();

        try {
            saveOrGet(ctx.service().deploy(cfg));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel(String name) throws GridException {
        A.notNull(name, "name");

        guard();

        try {
            saveOrGet(ctx.service().cancel(name));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancelAll() throws GridException {
        guard();

        try {
            saveOrGet(ctx.service().cancelAll());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridServiceDescriptor> deployedServices() {
        guard();

        try {
            return ctx.service().deployedServices();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T service(String name) {
        guard();

        try {
            return ctx.service().service(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Collection<T> services(String name) {
        guard();

        try {
            return ctx.service().services(name);
        }
        finally {
            unguard();
        }
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

    /** {@inheritDoc} */
    @Override public GridServices enableAsync() {
        return (GridServices)super.enableAsync();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(prj);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        prj = (GridProjectionAdapter)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        return prj.services();
    }
}
