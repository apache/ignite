/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor;

import java.io.*;
import java.util.*;

/**
 * Visor tasks argument.
 */
public class VisorTaskArgument<A> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node IDs task should be mapped to. */
    private final Collection<UUID> nodes;

    /** Task argument. */
    private final A arg;

    /** Debug flag. */
    private final boolean debug;

    /**
     * Create Visor task argument.
     *
     * @param nodes Node IDs task should be mapped to.
     * @param arg Task argument.
     * @param debug Debug flag.
     */
    public VisorTaskArgument(Collection<UUID> nodes, A arg, boolean debug) {
        assert nodes != null;
        assert !nodes.isEmpty();

        this.nodes = nodes;
        this.arg = arg;
        this.debug = debug;
    }

    /**
     * Create Visor task argument with nodes, but without actual argument.
     *
     * @param nodes Node IDs task should be mapped to.
     * @param debug Debug flag.
     */
    public VisorTaskArgument(Collection<UUID> nodes, boolean debug) {
        this(nodes, null, debug);
    }

    /**
     * Create Visor task argument.
     *
     * @param node Node ID task should be mapped to.
     * @param arg Task argument.
     * @param debug Debug flag.
     */
    public VisorTaskArgument(UUID node, A arg, boolean debug) {
        this(Collections.singleton(node), arg, debug);
    }

    /**
     * Create Visor task argument with nodes, but without actual argument.
     *
     * @param node Node ID task should be mapped to.
     * @param debug Debug flag.
     */
    public VisorTaskArgument(UUID node, boolean debug) {
        this(node, null, debug);
    }

    /**
     * @return Node IDs task should be mapped to.
     */
    public Collection<UUID> nodes() {
        return nodes;
    }

    /**
     * @return Task argument.
     */
    public A argument() {
        return arg;
    }

    /**
     * @return Debug flag.
     */
    public boolean debug() {
        return debug;
    }
}
