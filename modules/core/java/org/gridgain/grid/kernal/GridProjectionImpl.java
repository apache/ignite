// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;

import java.io.*;
import java.util.*;

/**
 * @author @java.author
 * @version @java.version
 */
public class GridProjectionImpl extends GridProjectionAdapter implements Externalizable {
    /** */
    private static final UUID[] EMPTY = new UUID[0];

    /** Type alias. */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    private static class Stash extends GridTuple3<String, GridPredicate<GridNode>, Boolean> { /*No-op.*/ }

    /** */
    private static final ThreadLocal<Stash> stash = new ThreadLocal<Stash>() {
        @Override protected Stash initialValue() {
            return new Stash();
        }
    };

    /** */
    private GridPredicate<GridNode> p;

    /** */
    private int hash = -1;

    /** */
    private boolean dynamic;

    /** */
    private UUID[] ids;

    /**
     * No-arg constructor is required by externalization.
     */
    public GridProjectionImpl() {
        super(null);
    }

    /**
     * Creates static projection.
     *
     * @param parent Parent projection.
     * @param ctx Kernal context.
     * @param nodes Collection of nodes for this subgrid.
     */
    public GridProjectionImpl(GridProjection parent, GridKernalContext ctx, Collection<GridNode> nodes) {
        this(parent, ctx, F.isEmpty(nodes) ? EMPTY : U.toArray(F.nodeIds(nodes), new UUID[nodes.size()]));
    }

    /**
     * Creates static projection.
     *
     * @param parent Parent projection.
     * @param ctx Kernal context.
     * @param nodeIds Node ids.
     */
    public GridProjectionImpl(GridProjection parent, GridKernalContext ctx, UUID[] nodeIds) {
        super(parent, ctx);

        assert nodeIds != null;

        ids = F.isEmpty(nodeIds) ? EMPTY : Arrays.copyOf(nodeIds, nodeIds.length);

        p = new GridNodePredicate<>(ids);

        dynamic = false;
    }

    /**
     * Creates dynamic projection.
     *
     * @param parent Projection.
     * @param ctx Kernal context.
     * @param p Predicate.
     */
    public GridProjectionImpl(GridProjection parent, GridKernalContext ctx, GridPredicate<? super GridNode> p) {
        super(parent, ctx);

        assert p != null;

        this.p = ctx.config().isPeerClassLoadingEnabled() ?
            new ProjectionPredicate(p) : (GridPredicate<GridNode>)p;

        dynamic = true;
    }

    /** {@inheritDoc} */
    @Override public GridPredicate<GridNode> predicate() {
        guard();

        try {
            return p;
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        // Dynamic subgrids use their predicates' hash code.
        return dynamic ? p.hashCode() : hash == -1 ? hash = Arrays.hashCode(ids) : hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof GridProjectionImpl)) {
            return false;
        }

        GridProjectionImpl obj = (GridProjectionImpl)o;

        // Dynamic subgrids use equality of their predicates.
        // For non-dynamic subgrids we use *initial* node IDs. The assumption
        // is that if the node leaves - it leaves all subgrids, and new node can't join existing
        // non-dynamic subgrid. Therefore, it is safe and effective to compare two non-dynamic
        // subgrids by their initial set of node IDs.
        return dynamic ? obj.dynamic && p.equals(obj.p) : !obj.dynamic && Arrays.equals(ids, obj.ids);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> nodes() {
        guard();

        try {
            return dynamic ? F.view(ctx.discovery().allNodes(), p) : F.view(ctx.discovery().nodes(F.asList(ids)), p);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean dynamic() {
        lightCheck();

        return dynamic;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, ctx.gridName());
        out.writeObject(p);
        out.writeBoolean(dynamic);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        GridTuple3<String, GridPredicate<GridNode>, Boolean> t = stash.get();

        t.set1(U.readString(in));
        t.set2((GridPredicate<GridNode>)in.readObject());
        t.set3(in.readBoolean());
    }

    /**
     * Reconstructs object on demarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of demarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            GridTuple3<String, GridPredicate<GridNode>, Boolean> t = stash.get();

            Grid g = GridFactoryEx.gridx(t.get1());

            // Preserve dynamic nature of the subgrid on demarshalling.
            return t.get3() ? g.forPredicate(t.get2()) :
                g.forNodes(g.forPredicate(t.get2()).nodes());
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridProjectionImpl.class, this);
    }

    /**
     * Projection predicate.
     */
    private static class ProjectionPredicate extends GridPredicate<GridNode> {
        /** Node filter. */
        private final GridPredicate<? super GridNode> p;

        /**
         * @param p Node filter.
         */
        private ProjectionPredicate(GridPredicate<? super GridNode> p) {
            this.p = p;

            peerDeployLike(p);
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridNode n) {
            return p.apply(n);
        }
    }
}
