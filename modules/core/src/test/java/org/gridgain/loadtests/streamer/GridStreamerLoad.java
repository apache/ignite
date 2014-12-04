/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.streamer;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.streamer.*;

import java.util.*;

/**
 * Configurable streamer load.
 */
public class GridStreamerLoad {
    /** Steamer name. */
    private String name;

    /** Load closures. */
    private List<IgniteInClosure<GridStreamer>> clos;

    /**
     * @return Steamer name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Steamer name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Query closure.
     */
    public List<IgniteInClosure<GridStreamer>> getClosures() {
        return clos;
    }

    /**
     * @param clos Query closure.
     */
    public void setClosures(List<IgniteInClosure<GridStreamer>> clos) {
        this.clos = clos;
    }
}
