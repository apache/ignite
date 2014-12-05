/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.streamer.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Stage wrapper that handles metrics calculation and time measurement.
 */
public class StreamerStageWrapper implements StreamerStage<Object> {
    /** Stage delegate. */
    private StreamerStage<Object> delegate;

    /** Stage index. */
    private int idx;

    /** Next stage name. Set after creation. */
    private String nextStageName;

    /**
     * @param delegate Delegate stage.
     * @param idx Index.
     */
    public StreamerStageWrapper(StreamerStage<Object> delegate, int idx) {
        this.delegate = delegate;
        this.idx = idx;
    }

    /**
     * @return Stage index.
     */
    public int index() {
        return idx;
    }

    /**
     * @return Next stage name in pipeline or {@code null} if this is the last stage.
     */
    @Nullable public String nextStageName() {
        return nextStageName;
    }

    /**
     * @param nextStageName Next stage name in pipeline.
     */
    public void nextStageName(String nextStageName) {
        this.nextStageName = nextStageName;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<?>> run(StreamerContext ctx, Collection<Object> evts)
        throws GridException {
        return delegate.run(ctx, evts);
    }

    /**
     * @return Delegate.
     */
    public StreamerStage unwrap() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StreamerStageWrapper.class, this);
    }
}
