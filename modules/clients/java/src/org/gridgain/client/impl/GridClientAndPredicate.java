/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;

/**
 * AND predicate. Passes if and only if both provided filters accept the node.
 * This filter uses short-term condition evaluation, i.e. second filter would not
 * be invoked if first filter returned {@code false}.
 */
class GridClientAndPredicate<T> implements GridClientPredicate<T> {
    /** First filter to check. */
    private GridClientPredicate<? super T> first;

    /** Second filter to check. */
    private GridClientPredicate<? super T> second;

    /**
     * Creates AND filter.
     *
     * @param first First filter to check.
     * @param second Second filter to check.
     */
    GridClientAndPredicate(GridClientPredicate<? super T> first, GridClientPredicate<? super T> second) {
        assert first != null;
        assert second != null;

        this.first = first;
        this.second = second;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(T elem) {
        return first.apply(elem) && second.apply(elem);
    }
}
