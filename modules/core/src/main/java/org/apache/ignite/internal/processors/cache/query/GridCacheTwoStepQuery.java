/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Two step map-reduce style query.
 */
public class GridCacheTwoStepQuery implements Serializable {
    /** */
    @GridToStringInclude
    private Map<String, GridCacheSqlQuery> mapQrys;

    /** */
    @GridToStringInclude
    private GridCacheSqlQuery reduce;

    /**
     * @param qry Reduce query.
     * @param params Reduce query parameters.
     */
    public GridCacheTwoStepQuery(String qry, Object ... params) {
        reduce = new GridCacheSqlQuery(null, qry, params);
    }

    /**
     * @param alias Alias.
     * @param qry SQL Query.
     * @param params Query parameters.
     */
    public void addMapQuery(String alias, String qry, Object ... params) {
        A.ensure(!F.isEmpty(alias), "alias must not be empty");

        if (mapQrys == null)
            mapQrys = new GridLeanMap<>();

        if (mapQrys.put(alias, new GridCacheSqlQuery(alias, qry, params)) != null)
            throw new IgniteException("Failed to add query, alias already exists: " + alias + ".");
    }

    /**
     * @return Reduce query.
     */
    public GridCacheSqlQuery reduceQuery() {
        return reduce;
    }

    /**
     * @return Map queries.
     */
    public Collection<GridCacheSqlQuery> mapQueries() {
        return mapQrys.values();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTwoStepQuery.class, this);
    }
}
