/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tests.p2p;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Projection predicate for event consume test.
 */
public class GridEventConsumeProjectionPredicate implements PN {
    /** {@inheritDoc} */
    @Override public boolean apply(ClusterNode n) {
        return true;
    }
}
