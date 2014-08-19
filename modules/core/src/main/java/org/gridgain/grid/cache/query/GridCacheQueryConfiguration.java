/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import java.io.*;
import java.util.*;

/**
 * Query configuration object.
 */
public class GridCacheQueryConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collection of query type metadata. */
    private Collection<GridCacheQueryTypeMetadata> typeMeta;

    /** Query type resolver. */
    private GridCacheQueryTypeResolver typeRslvr;

    /**
     * Default constructor.
     */
    public GridCacheQueryConfiguration() {
        // No-op.
    }

    /**
     * @param cfg Configuration to copy.
     */
    public GridCacheQueryConfiguration(GridCacheQueryConfiguration cfg) {
        typeMeta = cfg.getTypeMetadata();
        typeRslvr = cfg.getTypeResolver();
    }

    /**
     * Gets collection of query type metadata objects.
     *
     * @return Collection of query type metadata.
     */
    public Collection<GridCacheQueryTypeMetadata> getTypeMetadata() {
        return typeMeta;
    }

    /**
     * Sets collection of query type metadata objects.
     *
     * @param typeMeta Collection of query type metadata.
     */
    public void setTypeMetadata(Collection<GridCacheQueryTypeMetadata> typeMeta) {
        this.typeMeta = typeMeta;
    }

    /**
     * Gets query type resolver.
     *
     * @return Query type resolver.
     */
    public GridCacheQueryTypeResolver getTypeResolver() {
        return typeRslvr;
    }

    /**
     * Sets query type resolver.
     *
     * @param typeRslvr Query type resolver.
     */
    public void setTypeResolver(GridCacheQueryTypeResolver typeRslvr) {
        this.typeRslvr = typeRslvr;
    }
}
