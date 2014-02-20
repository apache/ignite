// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.clock;

/**
 * Interface representing time source for time processor.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridClockSource {
    /**
     * Gets current time in milliseconds past since 1 January, 1970.
     *
     * @return Current time in milliseconds.
     */
    public long currentTimeMillis();
}
