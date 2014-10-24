// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design;

import org.gridgain.grid.design.cluster.*;

import java.util.concurrent.*;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface Ignite {
    public IgniteCluster cluster();

    public IgniteCompute compute();

    public IgniteCompute compute(ClusterGroup grp);

    public ExecutorService executorService();

    public ExecutorService executorService(ClusterGroup grp);

    public IgniteManaged userServices();

    public IgniteManaged userServices(ClusterGroup grp);

    public <K, V> IgniteCache<K, V> cache(String name);

    public <T> IgniteQueue<T> queue(String name);
}
