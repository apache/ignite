/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.tasks;

import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * TODO: Add class description.
 */
public class VisorTaskArgument<A> extends GridBiTuple<Collection<UUID>, A> {
    /** */
    private static final long serialVersionUID = 0L;

    public VisorTaskArgument(Collection<UUID> nodes, A arg) {
        super(nodes, arg);

        assert nodes != null;
        assert !nodes.isEmpty();
    }

    public VisorTaskArgument(Collection<UUID> nodes) {
        this(nodes, null);
    }

    public VisorTaskArgument(UUID node, A arg) {
        this(Collections.singleton(node), arg);
    }

    public VisorTaskArgument(UUID node) {
        this(node, null);
    }

    public Collection<UUID> nodes() {
        return get1();
    }

    public A argument() {
        return get2();
    }
}
