// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.service;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridServiceConfiguration implements Serializable {
    private String name;

    @GridToStringExclude
    private GridService svc;

    private int totalCnt;

    private int maxPerNode;

    private String cacheName;

    private Object affKey;

    @GridToStringExclude
    private GridPredicate<GridNode> nodeFilter;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public GridService getService() {
        return svc;
    }

    public void setService(GridService svc) {
        this.svc = svc;
    }

    public int getTotalCount() {
        return totalCnt;
    }

    public void setTotalCount(int totalCnt) {
        this.totalCnt = totalCnt;
    }

    public int getMaxPerNodeCount() {
        return maxPerNode;
    }

    public void setMaxPerNodeCount(int maxPerNode) {
        this.maxPerNode = maxPerNode;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public Object getAffinityKey() {
        return affKey;
    }

    public void setAffinityKey(Object affKey) {
        this.affKey = affKey;
    }

    public GridPredicate<GridNode> getNodeFilter() {
        return nodeFilter;
    }

    public void setNodeFilter(GridPredicate<GridNode> nodeFilter) {
        this.nodeFilter = nodeFilter;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GridServiceConfiguration that = (GridServiceConfiguration)o;

        if (maxPerNode != that.maxPerNode)
            return false;

        if (totalCnt != that.totalCnt)
            return false;

        if (affKey != null ? !affKey.equals(that.affKey) : that.affKey != null)
            return false;

        if (cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null)
            return false;

        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;

        if (nodeFilter != null ? !nodeFilter.getClass().equals(that.nodeFilter.getClass()) : that.nodeFilter != null)
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
