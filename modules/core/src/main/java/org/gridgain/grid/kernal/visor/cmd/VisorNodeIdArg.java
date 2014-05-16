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
 * Simplest {@link VisorOneNodeArg}  implementation for cases where task doesn't need any data on input.
 */
public class VisorNodeIdArg extends VisorOneNodeArg {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param nodeId Node Id.
     */
    public VisorNodeIdArg(UUID nodeId) {
        super(nodeId);
    }
}
