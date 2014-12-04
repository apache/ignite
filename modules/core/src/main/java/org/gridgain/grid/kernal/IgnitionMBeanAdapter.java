/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Management bean that provides access to {@link GridGain}.
 */
public class IgnitionMBeanAdapter implements IgnitionMBean {
    /** {@inheritDoc} */
    @Override public String getState() {
        return G.state().toString();
    }

    /** {@inheritDoc} */
    @Override public String getState(String name) {
        if (F.isEmpty(name))
            name = null;

        return G.state(name).toString();
    }

    /** {@inheritDoc} */
    @Override public boolean stop(boolean cancel) {
        return G.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public boolean stop(String name, boolean cancel) {
        return G.stop(name, cancel);
    }

    /** {@inheritDoc} */
    @Override public void stopAll(boolean cancel) {
        G.stopAll(cancel);
    }

    /** {@inheritDoc} */
    @Override public void restart(boolean cancel) {
        G.restart(cancel);
    }
}
