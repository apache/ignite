// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.service;

import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridServiceConfiguration {
    private GridService svc;

    private int totalCnt;

    private int maxPerNode;

    private String cacheName;

    private Collection<?> affKeys;

    private Collection<Integer> parts;

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

    public Collection<?> getAffinityKeys() {
        return affKeys;
    }

    public void setAffinityKeys(Collection<?> affKeys) {
        this.affKeys = affKeys;
    }

    public Collection<Integer> getPartitions() {
        return parts;
    }

    public void setPartitions(Collection<Integer> parts) {
        this.parts = parts;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceConfiguration.class, this);
    }
}
