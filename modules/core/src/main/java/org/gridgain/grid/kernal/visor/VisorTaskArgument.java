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

    /**
     * Create Visor task argument.
     *
     * @param nodes Node IDs task should be mapped to.
     * @param arg Task argument.
     */
    public VisorTaskArgument(Collection<UUID> nodes, A arg) {
        assert nodes != null;
        assert !nodes.isEmpty();

        this.nodes = nodes;
        this.arg = arg;
    }

    /**
     * Create Visor task argument with nodes, but without actual argument.
     *
     * @param nodes Node IDs task should be mapped to.
     */
    public VisorTaskArgument(Collection<UUID> nodes) {
        this(nodes, null);
    }

    /**
     * Create Visor task argument.
     *
     * @param node Node ID task should be mapped to.
     * @param arg Task argument.
     */
    public VisorTaskArgument(UUID node, A arg) {
        this(Collections.singleton(node), arg);
    }

    /**
     * Create Visor task argument with nodes, but without actual argument.
     *
     * @param node Node ID task should be mapped to.
     */
    public VisorTaskArgument(UUID node) {
        this(node, null);
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
}
