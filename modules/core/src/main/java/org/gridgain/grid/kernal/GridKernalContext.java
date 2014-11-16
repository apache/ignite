/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.design.plugin.*;
import org.gridgain.grid.kernal.managers.security.*;
import org.gridgain.grid.kernal.managers.checkpoint.*;
import org.gridgain.grid.kernal.managers.collision.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.managers.failover.*;
import org.gridgain.grid.kernal.managers.indexing.*;
import org.gridgain.grid.kernal.managers.loadbalancer.*;
import org.gridgain.grid.kernal.managers.securesession.*;
import org.gridgain.grid.kernal.managers.swapspace.*;
import org.gridgain.grid.kernal.processors.affinity.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.clock.*;
import org.gridgain.grid.kernal.processors.closure.*;
import org.gridgain.grid.kernal.processors.continuous.*;
import org.gridgain.grid.kernal.processors.dataload.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.gridgain.grid.kernal.processors.email.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.interop.*;
import org.gridgain.grid.kernal.processors.job.*;
import org.gridgain.grid.kernal.processors.jobmetrics.*;
import org.gridgain.grid.kernal.processors.license.*;
import org.gridgain.grid.kernal.processors.offheap.*;
import org.gridgain.grid.kernal.processors.port.*;
import org.gridgain.grid.kernal.processors.portable.*;
import org.gridgain.grid.kernal.processors.resource.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.schedule.*;
import org.gridgain.grid.kernal.processors.segmentation.*;
import org.gridgain.grid.kernal.processors.service.*;
import org.gridgain.grid.kernal.processors.session.*;
import org.gridgain.grid.kernal.processors.streamer.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.kernal.processors.version.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
@GridToStringExclude
public interface GridKernalContext extends GridMetadataAware, Iterable<GridComponent> {
    /**
     * Gets list of all grid components in the order they were added.
     *
     * @return List of all grid components in the order they were added.
     */
    public List<GridComponent> components();

    /**
     * Gets local node ID.
     *
     * @return Local node ID.
     */
    public UUID localNodeId();

    /**
     * Gets grid name.
     *
     * @return Grid name.
     */
    public String gridName();

    /**
     * Gets grid product.
     *
     * @return Grid product.
     */
    public GridProduct product();

    /**
     * Gets list of compatible versions.
     *
     * @return Compatible versions.
     */
    public Collection<String> compatibleVersions();

    /**
     * Gets logger.
     *
     * @return Logger.
     */
    public GridLogger log();

    /**
     * Gets logger for given class.
     *
     * @param cls Class to get logger for.
     * @return Logger.
     */
    public GridLogger log(Class<?> cls);

    /**
     * @return {@code True} if grid is in the process of stopping.
     */
    public boolean isStopping();

    /**
     * Gets kernal gateway.
     *
     * @return Kernal gateway.
     */
    public GridKernalGateway gateway();

    /**
     * Gets grid instance managed by kernal.
     *
     * @return Grid instance.
     */
    public GridEx grid();

    /**
     * Gets grid configuration.
     *
     * @return Grid configuration.
     */
    public GridConfiguration config();

    /**
     * Gets task processor.
     *
     * @return Task processor.
     */
    public GridTaskProcessor task();

    /**
     * Gets license processor.
     *
     * @return License processor.
     */
    public GridLicenseProcessor license();

    /**
     * Gets cache data affinity processor.
     *
     * @return Cache data affinity processor.
     */
    public GridAffinityProcessor affinity();

    /**
     * Gets job processor.
     *
     * @return Job processor
     */
    public GridJobProcessor job();

    /**
     * Gets offheap processor.
     *
     * @return Off-heap processor.
     */
    public GridOffHeapProcessor offheap();

    /**
     * Gets timeout processor.
     *
     * @return Timeout processor.
     */
    public GridTimeoutProcessor timeout();

    /**
     * Gets time processor.
     *
     * @return Time processor.
     */
    public GridClockSyncProcessor clockSync();

    /**
     * Gets resource processor.
     *
     * @return Resource processor.
     */
    public GridResourceProcessor resource();

    /**
     * Gets job metric processor.
     *
     * @return Metrics processor.
     */
    public GridJobMetricsProcessor jobMetric();

    /**
     * Gets caches processor.
     *
     * @return Cache processor.
     */
    public GridCacheProcessor cache();

    /**
     * Gets task session processor.
     *
     * @return Session processor.
     */
    public GridTaskSessionProcessor session();

    /**
     * Gets closure processor.
     *
     * @return Closure processor.
     */
    public GridClosureProcessor closure();

    /**
     * Gets service processor.
     *
     * @return Service processor.
     */
    public GridServiceProcessor service();

    /**
     * Gets port processor.
     *
     * @return Port processor.
     */
    public GridPortProcessor ports();

    /**
     * Gets email processor.
     *
     * @return Email processor.
     */
    public GridEmailProcessorAdapter email();

    /**
     * Gets schedule processor.
     *
     * @return Schedule processor.
     */
    public GridScheduleProcessorAdapter schedule();

    /**
     * Gets REST processor.
     *
     * @return REST processor.
     */
    public GridRestProcessor rest();

    /**
     * Gets segmentation processor.
     *
     * @return Segmentation processor.
     */
    public GridSegmentationProcessor segmentation();

    /**
     * Gets data loader processor.
     *
     * @return Data loader processor.
     */
    public <K, V> GridDataLoaderProcessor<K, V> dataLoad();

    /**
     * Gets file system processor.
     *
     * @return File system processor.
     */
    public GridGgfsProcessorAdapter ggfs();

    /**
     * Gets GGFS utils processor.
     *
     * @return GGFS utils processor.
     */
    public GridGgfsHelper ggfsHelper();

    /**
     * Gets stream processor.
     *
     * @return Stream processor.
     */
    public GridStreamProcessor stream();

    /**
     * Gets event continuous processor.
     *
     * @return Event continuous processor.
     */
    public GridContinuousProcessor continuous();

    /**
     * Gets Hadoop processor.
     *
     * @return Hadoop processor.
     */
    public GridHadoopProcessorAdapter hadoop();

    /**
     * Gets utility cache pool.
     *
     * @return DR pool.
     */
    public ExecutorService utilityCachePool();

    /**
     * Gets version converter processor.
     *
     * @return Version converter processor.
     */
    public GridVersionProcessor versionConverter();

    /**
     * Gets portable processor.
     *
     * @return Portable processor.
     */
    public GridPortableProcessor portable();

    /**
     * Gets interop processor.
     *
     * @return Interop processor.
     */
    public GridInteropProcessor interop();

    /**
     * Gets deployment manager.
     *
     * @return Deployment manager.
     */
    public GridDeploymentManager deploy();

    /**
     * Gets communication manager.
     *
     * @return Communication manager.
     */
    public GridIoManager io();

    /**
     * Gets discovery manager.
     *
     * @return Discovery manager.
     */
    public GridDiscoveryManager discovery();

    /**
     * Gets checkpoint manager.
     *
     * @return Checkpoint manager.
     */
    public GridCheckpointManager checkpoint();

    /**
     * Gets event storage manager.
     *
     * @return Event storage manager.
     */
    public GridEventStorageManager event();

    /**
     * Gets failover manager.
     *
     * @return Failover manager.
     */
    public GridFailoverManager failover();

    /**
     * Gets collision manager.
     *
     * @return Collision manager.
     */
    public GridCollisionManager collision();

    /**
     * Gets authentication manager.
     *
     * @return Authentication manager.
     */
    public GridSecurityManager security();

    /**
     * Gets secure session manager.
     *
     * @return Secure session manager.
     */
    public GridSecureSessionManager secureSession();

    /**
     * Gets load balancing manager.
     *
     * @return Load balancing manager.
     */
    public GridLoadBalancerManager loadBalancing();

    /**
     * Gets swap space manager.
     *
     * @return Swap space manager.
     */
    public GridSwapSpaceManager swap();

    /**
     * Gets indexing manager.
     *
     * @return Indexing manager.
     */
    public GridIndexingManager indexing();

    /**
     * Gets grid time source.
     *
     * @return Time source.
     */
    public GridClockSource timeSource();

    /**
     * Sets segmented flag to {@code true} when node is stopped due to segmentation issues.
     */
    public void markSegmented();

    /**
     * Gets segmented flag.
     *
     * @return {@code True} if network is currently segmented, {@code false} otherwise.
     */
    public boolean segmented();

    /**
     * Print grid kernal memory stats (sizes of internal structures, etc.).
     *
     * NOTE: This method is for testing and profiling purposes only.
     */
    public void printMemoryStats();

    /**
     * Checks whether this node is daemon.
     *
     * @return {@code True} if this node is daemon, {@code false} otherwise.
     */
    public boolean isDaemon();

    /**
     * @return Performance suggestions object.
     */
    public GridPerformanceSuggestions performance();

    /**
     * @return Enterprise release flag.
     */
    public boolean isEnterprise();

    /**
     * Gets user version for given class loader by checking
     * {@code META-INF/gridgain.xml} file for {@code userVersion} attribute. If
     * {@code gridgain.xml} file is not found, or user version is not specified there,
     * then default version (empty string) is returned.
     *
     * @param ldr Class loader.
     * @return User version for given class loader or empty string if no version
     *      was explicitly specified.
     */
    public String userVersion(ClassLoader ldr);

    /**
     * @param name Plugin name.
     * @return Plugin provider instance.
     * @throws PluginNotFoundException If plugin provider for the given name was not found.
     */
    public PluginProvider pluginProvider(String name) throws PluginNotFoundException;

    /**
     * Creates optional component.
     *
     * @param cls Component class.
     * @return Created component.
     */
    public <T> T createComponent(Class<T> cls);

    /**
     * @return Message factory.
     */
    public GridTcpMessageFactory messageFactory();

    /**
     * @param producer Message producer.
     * @return Message type code.
     */
    public byte registerMessageProducer(GridTcpCommunicationMessageProducer producer);

    /**
     * @return Plugin manager.
     */
    public IgnitePluginManager pluginManager();
}
