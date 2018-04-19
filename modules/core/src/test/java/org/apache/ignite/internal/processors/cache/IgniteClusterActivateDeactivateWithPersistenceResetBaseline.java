/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

/**
 * Variant of {@link IgniteClusterActivateDeactivateTestWithPersistence} that resets baseline after topology changes
 * in certain test cases.
 */
public class IgniteClusterActivateDeactivateWithPersistenceResetBaseline extends IgniteClusterActivateDeactivateTestWithPersistence {
    /** {@inheritDoc} */
    @Override protected boolean needToResetBaselineTopology() {
        return true;
    }
}
