/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.security.*;

import java.util.*;

/**
 * Security context implementation.
 */
public class GridSecurityContextImpl implements GridSecurityContext {
    /** Security operations. */
    private Collection<GridSecurityOperation> ops;

    /** Authentication subject. */
    private Object subj;

    /**
     * @param ops Operations.
     * @param subj Subject.
     */
    public GridSecurityContextImpl(Collection<GridSecurityOperation> ops, Object subj) {
        this.ops = ops;
        this.subj = subj;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridSecurityOperation> operations() {
        return ops;
    }

    /** {@inheritDoc} */
    @Override public Object subject() {
        return subj;
    }
}
