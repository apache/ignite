/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import java.util.*;

/**
 * Arguments for tasks that require set of names.
 */
public class VisorOneNodeNamesArg extends VisorOneNodeArg {
    /** */
    private static final long serialVersionUID = 0L;

    /** Names to process in task. */
    private final Set<String> names;

    /**
     * Create arguments with given parameters.
     *
     * @param nid Node Id.
     * @param names Set of names.
     */
    public VisorOneNodeNamesArg(UUID nid, Set<String> names) {
        super(nid);

        this.names = names;
    }

    /**
     * @return Names to process.
     */
    public Set<String> names() {
        return names;
    }
}
