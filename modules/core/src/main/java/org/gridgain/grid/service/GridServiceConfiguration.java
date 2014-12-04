/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.service;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Managed service configuration. In addition to deploying managed services by
 * calling any of the provided {@code deploy(...)} methods, managed services
 * can also be automatically deployed on startup by specifying them in {@link org.apache.ignite.configuration.IgniteConfiguration}
 * like so:
 * <pre name="code" class="java">
 * GridConfiguration gridCfg = new GridConfiguration();
 *
 * GridServiceConfiguration svcCfg1 = new GridServiceConfiguration();
 *
 * svcCfg1.setName("myClusterSingletonService");
 * svcCfg1.setMaxPerNodeCount(1);
 * svcCfg1.setTotalCount(1);
 * svcCfg1.setService(new MyClusterSingletonService());
 *
 * GridServiceConfiguration svcCfg2 = new GridServiceConfiguration();
 *
 * svcCfg2.setName("myNodeSingletonService");
 * svcCfg2.setMaxPerNodeCount(1);
 * svcCfg2.setService(new MyNodeSingletonService());
 *
 * gridCfg.setServiceConfiguration(svcCfg1, svcCfg2);
 * ...
 * GridGain.start(gridCfg);
 * </pre>
 * The above configuration can also be specified in a Spring configuration file.
 */
public class GridServiceConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private String name;

    /** Service instance. */
    @GridToStringExclude
    private GridService svc;

    /** Total count. */
    private int totalCnt;

    /** Max per-node count. */
    private int maxPerNodeCnt;

    /** Cache name. */
    private String cacheName;

    /** Affinity key. */
    private Object affKey;

    /** Node filter. */
    @GridToStringExclude
    private GridPredicate<ClusterNode> nodeFilter;

    /**
     * Gets service name.
     * <p>
     * This parameter is mandatory when deploying a service.
     *
     * @return Service name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets service name.
     * <p>
     * This parameter is mandatory when deploying a service.
     *
     * @param name Service name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets service instance.
     * <p>
     * This parameter is mandatory when deploying a service.
     *
     * @return Service instance.
     */
    public GridService getService() {
        return svc;
    }

    /**
     * Sets service instance.
     * <p>
     * This parameter is mandatory when deploying a service.
     *
     * @param svc Service instance.
     */
    public void setService(GridService svc) {
        this.svc = svc;
    }

    /**
     * Gets total number of deployed service instances in the cluster, {@code 0} for unlimited.
     * <p>
     * At least one of {@code getTotalCount()} or {@link #getMaxPerNodeCount()} values must be positive.
     *
     * @return Total number of deployed service instances in the cluster, {@code 0} for unlimited.
     */
    public int getTotalCount() {
        return totalCnt;
    }

    /**
     * Sets total number of deployed service instances in the cluster, {@code 0} for unlimited.
     * <p>
     * At least one of {@code getTotalCount()} or {@link #getMaxPerNodeCount()} values must be positive.
     *
     * @param totalCnt Total number of deployed service instances in the cluster, {@code 0} for unlimited.
     */
    public void setTotalCount(int totalCnt) {
        this.totalCnt = totalCnt;
    }

    /**
     * Gets maximum number of deployed service instances on each node, {@code 0} for unlimited.
     * <p>
     * At least one of {@code getMaxPerNodeCount()} or {@link #getTotalCount()} values must be positive.
     *
     * @return Maximum number of deployed service instances on each node, {@code 0} for unlimited.
     */
    public int getMaxPerNodeCount() {
        return maxPerNodeCnt;
    }

    /**
     * Sets maximum number of deployed service instances on each node, {@code 0} for unlimited.
     * <p>
     * At least one of {@code getMaxPerNodeCount()} or {@link #getTotalCount()} values must be positive.
     *
     * @param maxPerNodeCnt Maximum number of deployed service instances on each node, {@code 0} for unlimited.
     */
    public void setMaxPerNodeCount(int maxPerNodeCnt) {
        this.maxPerNodeCnt = maxPerNodeCnt;
    }

    /**
     * Gets cache name used for key-to-node affinity calculation.
     * <p>
     * This parameter is optional and is set only when deploying service based on key-affinity.
     *
     * @return Cache name, possibly {@code null}.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Sets cache name used for key-to-node affinity calculation.
     * <p>
     * This parameter is optional and is set only when deploying service based on key-affinity.
     *
     * @param cacheName Cache name, possibly {@code null}.
     */
    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * Gets affinity key used for key-to-node affinity calculation.
     * <p>
     * This parameter is optional and is set only when deploying service based on key-affinity.
     *
     * @return Affinity key, possibly {@code null}.
     */
    public Object getAffinityKey() {
        return affKey;
    }

    /**
     * Sets affinity key used for key-to-node affinity calculation.
     * <p>
     * This parameter is optional and is set only when deploying service based on key-affinity.
     *
     * @param affKey Affinity key, possibly {@code null}.
     */
    public void setAffinityKey(Object affKey) {
        this.affKey = affKey;
    }

    /**
     * Gets node filter used to filter nodes on which the service will be deployed.
     * <p>
     * This parameter is optional. If not provided service may be deployed on any or all
     * nodes in the grid, based on configuration.
     *
     * @return Node filter used to filter nodes on which the service will be deployed, possibly {@code null}.
     */
    public GridPredicate<ClusterNode> getNodeFilter() {
        return nodeFilter;
    }

    /**
     * Sets node filter used to filter nodes on which the service will be deployed.
     * <p>
     * This parameter is optional. If not provided service may be deployed on any or all
     * nodes in the grid, based on configuration.
     *
     * @param nodeFilter Node filter used to filter nodes on which the service will be deployed, possibly {@code null}.
     */
    public void setNodeFilter(GridPredicate<ClusterNode> nodeFilter) {
        this.nodeFilter = nodeFilter;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantIfStatement", "EqualsWhichDoesntCheckParameterClass"})
    @Override public boolean equals(Object o) {
        if (!equalsIgnoreNodeFilter(o))
            return false;

        GridServiceConfiguration that = (GridServiceConfiguration)o;

        if (nodeFilter != null && that.nodeFilter != null) {
            if (!nodeFilter.getClass().equals(that.nodeFilter.getClass()))
                return false;
        }
        else if (nodeFilter != null || that.nodeFilter != null)
            return false;

        return true;
    }

    /**
     * Checks if configurations are equal ignoring the node filter. Node filters control on which
     * nodes the services are deployed and often can be ignored for equality checks.
     *
     * @param o Other configuration.
     * @return {@code True} if configurations are equal, {@code false} otherwise.
     */
    @SuppressWarnings("RedundantIfStatement")
    public boolean equalsIgnoreNodeFilter(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridServiceConfiguration that = (GridServiceConfiguration)o;

        if (maxPerNodeCnt != that.maxPerNodeCnt)
            return false;

        if (totalCnt != that.totalCnt)
            return false;

        if (affKey != null ? !affKey.equals(that.affKey) : that.affKey != null)
            return false;

        if (cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null)
            return false;

        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;

        if (svc != null ? !svc.getClass().equals(that.svc.getClass()) : that.svc != null)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        String svcCls = svc == null ? "" : svc.getClass().getSimpleName();
        String nodeFilterCls = nodeFilter == null ? "" : nodeFilter.getClass().getSimpleName();

        return S.toString(GridServiceConfiguration.class, this, "svcCls", svcCls, "nodeFilterCls", nodeFilterCls);
    }
}
