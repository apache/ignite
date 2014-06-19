// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.service;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridServiceContext {
    public UUID serviceId();

    public boolean isCancelled();

    public int totalCount();

    public int maxPerNodeCount();

    @Nullable public String cacheName();

    public <K> Collection<? extends K> affinityKeys();

    public Collection<Integer> partitions();
}
