/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.streamer;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

/**
 * Closure for events generation.
 */
class EventClosure implements IgniteInClosure<GridStreamer> {
    /** Random range. */
    private int rndRange = 100;

    /** {@inheritDoc} */
    @Override public void apply(GridStreamer streamer) {
        Random rnd = new Random();

        while (!Thread.interrupted()) {
            try {
                streamer.addEvent(rnd.nextInt(rndRange));
            }
            catch (GridException e) {
                X.println("Failed to add streamer event: " + e);
            }
        }
    }

    /**
     * @return Random range.
     */
    public int getRandomRange() {
        return rndRange;
    }

    /**
     * @param rndRange Random range.
     */
    public void setRandomRange(int rndRange) {
        this.rndRange = rndRange;
    }
}
