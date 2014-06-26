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
    public GridProjection projection();

    public GridFuture<?> deployOnEachNode(String name, GridService svc);

    public GridFuture<?> deploySingleton(String name, GridService svc);

    public GridFuture<?> deployMultiple(String name, GridService svc, int totalCnt, int maxPerNodeCnt);

    public GridFuture<?> deployForAffinityKey(String name, GridService svc, String cacheName, Object affKey);

    public GridFuture<?> deploy(GridServiceConfiguration cfg);

    public GridFuture<?> cancel(String name);

    public GridFuture<?> cancelAll();

    public Collection<GridServiceDescriptor> deployedServices();
}
