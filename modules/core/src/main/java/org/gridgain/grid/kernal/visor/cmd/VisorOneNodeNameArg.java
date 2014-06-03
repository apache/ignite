/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import java.util.UUID;

/**
 * Argument for task that require some name (cache name, streamer name, ...).
 */
public class VisorOneNodeNameArg extends VisorOneNodeArg {
    /** */
    private static final long serialVersionUID = 0L;

    /** Name. */
    private final String name;

    /**
     * Create argument with given parameters.
     *
     * @param nodeId Node Id.
     * @param name Argument value.
     */
    public VisorOneNodeNameArg(UUID nodeId, String name) {
        super(nodeId);

        this.name = name;
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }
}
