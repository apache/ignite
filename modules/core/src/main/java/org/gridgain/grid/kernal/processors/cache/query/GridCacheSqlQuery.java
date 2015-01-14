/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Query.
 */
public class GridCacheSqlQuery implements Externalizable {
    /** */
    private static final Object[] EMPTY_PARAMS = {};

    /** */
    String alias;

    /** */
    @GridToStringInclude
    String qry;

    /** */
    @GridToStringInclude
    Object[] params;

    /**
     * For {@link Externalizable}.
     */
    public GridCacheSqlQuery() {
        // No-op.
    }

    /**
     * @param alias Alias.
     * @param qry Query.
     * @param params Query parameters.
     */
    GridCacheSqlQuery(String alias, String qry, Object[] params) {
        A.ensure(!F.isEmpty(qry), "qry must not be empty");

        this.alias = alias;
        this.qry = qry;

        this.params = F.isEmpty(params) ? EMPTY_PARAMS : params;
    }

    /**
     * @return Alias.
     */
    public String alias() {
        return alias;
    }

    /**
     * @return Query.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Parameters.
     */
    public Object[] parameters() {
        return params;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, alias);
        U.writeString(out, qry);
        U.writeArray(out, params);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        alias = U.readString(in);
        qry = U.readString(in);
        params = U.readArray(in);

        if (F.isEmpty(params))
            params = EMPTY_PARAMS;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSqlQuery.class, this);
    }
}
