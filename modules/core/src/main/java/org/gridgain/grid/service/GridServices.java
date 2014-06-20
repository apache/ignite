// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.service;

import org.gridgain.grid.*;

import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridServices {
    public GridFuture<UUID> deployOnEachNode(GridService svc);

    public GridFuture<UUID> deploySingleton(GridService svc);

    public GridFuture<UUID> deployMultiple(GridService svc, int totalCnt, int maxPerNodeCnt);

    public <K> GridFuture<UUID> deployForAffinityKey(GridService svc, String cacheName, K affKey,
        boolean includeBackups);

    public GridFuture<?> cancel(UUID svcId);

    public Collection<? extends GridServiceDescriptor> deployedServices();
}
