/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Predicate for query over {@link GridCacheSet} items.
 */
public class GridSetQueryPredicate<K, V> implements IgniteBiPredicate<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridUuid setId;

    /** */
    private boolean collocated;

    /** */
    private GridCacheContext ctx;

    /** */
    private boolean filter;

    /**
     * Required by {@link Externalizable}.
     */
    public GridSetQueryPredicate() {
        // No-op.
    }

    /**
     * @param setId Set ID.
     * @param collocated Collocation flag.
     */
    public GridSetQueryPredicate(GridUuid setId, boolean collocated) {
        this.setId = setId;
        this.collocated = collocated;
    }

    /**
     * @param ctx Cache context.
     */
    public void init(GridCacheContext ctx) {
        this.ctx = ctx;

        filter = filterKeys();
    }

    /**
     *
     * @return Collocation flag.
     */
    public boolean collocated() {
        return collocated;
    }

    /**
     * @return Set ID.
     */
    public GridUuid setId() {
        return setId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean apply(K k, V v) {
        return !filter || ctx.affinity().primary(ctx.localNode(), k, ctx.affinity().affinityTopologyVersion());
    }

    /**
     * @return {@code True} if need to filter out non-primary keys during processing of set data query.
     */
    private boolean filterKeys() {
        return !collocated && !(ctx.isLocal() || ctx.isReplicated()) &&
            (ctx.config().getBackups() > 0 || CU.isNearEnabled(ctx));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, setId);
        out.writeBoolean(collocated);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setId = U.readGridUuid(in);
        collocated = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSetQueryPredicate.class, this);
    }
}
