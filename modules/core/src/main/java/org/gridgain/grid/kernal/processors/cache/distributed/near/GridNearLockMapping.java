/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Key-to node mapping.
 */
public class GridNearLockMapping<K, V> {
    /** Node to which keys are mapped. */
    private ClusterNode node;

    /** Collection of mapped keys. */
    @GridToStringInclude
    private Collection<K> mappedKeys = new LinkedList<>();

    /** Near lock request. */
    @GridToStringExclude
    private GridNearLockRequest<K, V> req;

    /** Distributed keys. Key will not be distributed if lock is reentry. */
    @GridToStringInclude
    private Collection<K> distributedKeys;

    /**
     * Creates near lock mapping for specified node and key.
     *
     * @param node Node.
     * @param firstKey First key in mapped keys collection.
     */
    public GridNearLockMapping(ClusterNode node, K firstKey) {
        assert node != null;
        assert firstKey != null;

        this.node = node;

        mappedKeys.add(firstKey);
    }

    /**
     * @return Node to which keys are mapped.
     */
    public ClusterNode node() {
        return node;
    }

    /**
     * @return Mapped keys.
     */
    public Collection<K> mappedKeys() {
        return mappedKeys;
    }

    /**
     * @param key Key to add to mapping.
     */
    public void addKey(K key) {
        mappedKeys.add(key);
    }

    /**
     * @return Near lock request.
     */
    @Nullable public GridNearLockRequest<K, V> request() {
        return req;
    }

    /**
     * @param req Near lock request.
     */
    public void request(GridNearLockRequest<K, V> req) {
        assert req != null;

        this.req = req;
    }

    /**
     * @return Collection of distributed keys.
     */
    public Collection<K> distributedKeys() {
        return distributedKeys;
    }

    /**
     * @param distributedKeys Collection of distributed keys.
     */
    public void distributedKeys(Collection<K> distributedKeys) {
        this.distributedKeys = distributedKeys;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridNearLockMapping.class, this);
    }
}
