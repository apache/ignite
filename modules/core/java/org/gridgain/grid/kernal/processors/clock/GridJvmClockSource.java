// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.clock;

/**
 * JVM time source.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridJvmClockSource implements GridClockSource {
    /** {@inheritDoc} */
    @Override public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
