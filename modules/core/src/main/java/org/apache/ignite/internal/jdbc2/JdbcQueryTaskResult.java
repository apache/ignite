/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.jdbc2;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 * Result of query execution.
 */
class JdbcQueryTaskResult implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Uuid. */
    private final UUID uuid;

    /** Finished. */
    private final boolean finished;

    /** Result type - query or update. */
    private final boolean isQry;

    /** Rows. */
    private final List<List<?>> rows;

    /** Tables. */
    private final List<String> tbls;

    /** Columns. */
    private final List<String> cols;

    /** Types. */
    private final List<String> types;

    /**
     * @param uuid UUID..
     * @param finished Finished.
     * @param isQry Is query flag.
     * @param rows Rows.
     * @param cols Columns.
     * @param tbls Tables.
     * @param types Types.
     */
    public JdbcQueryTaskResult(UUID uuid, boolean finished, boolean isQry, List<List<?>> rows, List<String> cols,
        List<String> tbls, List<String> types) {
        this.isQry = isQry;
        this.cols = cols;
        this.uuid = uuid;
        this.finished = finished;
        this.rows = rows;
        this.tbls = tbls;
        this.types = types;
    }

    /**
     * @return Query result rows.
     */
    public List<List<?>> getRows() {
        return rows;
    }

    /**
     * @return Tables metadata.
     */
    public List<String> getTbls() {
        return tbls;
    }

    /**
     * @return Columns metadata.
     */
    public List<String> getCols() {
        return cols;
    }

    /**
     * @return Types metadata.
     */
    public List<String> getTypes() {
        return types;
    }

    /**
     * @return Query UUID.
     */
    public UUID getUuid() {
        return uuid;
    }

    /**
     * @return {@code True} if it is finished query.
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * @return {@code true} if it is result of a query operation, not update; {@code false} otherwise.
     */
    public boolean isQuery() {
        return isQry;
    }
}
