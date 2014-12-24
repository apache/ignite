/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

/**
 * Clock timer for tests.
 */
public class GridTestClockTimer implements Runnable {
    /** {@inheritDoc} */
    @Override public void run() {
        while (true) {
            GridUtils.curTimeMillis = System.currentTimeMillis();

            try {
                Thread.sleep(10);
            }
            catch (InterruptedException ignored) {
                GridUtils.log(null, "Timer thread has been interrupted.");

                break;
            }
        }
    }
}
