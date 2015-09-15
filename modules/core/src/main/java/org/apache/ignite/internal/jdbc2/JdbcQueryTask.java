/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.jdbc2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Task for SQL queries execution through {@link IgniteJdbcDriver}.
 * <p>
 * Not closed cursors will be removed after {@link #RMV_DELAY} milliseconds.
 * This parameter can be configured via {@link IgniteSystemProperties#IGNITE_JDBC_DRIVER_CURSOR_REMOVE_DELAY}
 * system property.
 */
class JdbcQueryTask implements IgniteCallable<JdbcQueryTask.QueryResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** How long to store open cursor. */
    private static final long RMV_DELAY = IgniteSystemProperties.getLong(
        IgniteSystemProperties.IGNITE_JDBC_DRIVER_CURSOR_REMOVE_DELAY, 600000);

    /** Scheduler. */
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);

    /** Open cursors. */
    private static final ConcurrentMap<UUID, Cursor> CURSORS = new ConcurrentHashMap<>();

    /** Ignite. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Uuid. */
    private final UUID uuid;

    /** Cache name. */
    private final String cacheName;

    /** Sql. */
    private final String sql;

    /** Args. */
    private final Object[] args;

    /** Fetch size. */
    private final int fetchSize;

    /** Local execution flag. */
    private final boolean loc;

    /** Local query flag. */
    private final boolean locQry;

    /** Collocated query flag. */
    private final boolean collocatedQry;

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param sql Sql query.
     * @param loc Local execution flag.
     * @param args Args.
     * @param fetchSize Fetch size.
     * @param uuid UUID.
     * @param locQry Local query flag.
     * @param collocatedQry Collocated query flag.
     */
    public JdbcQueryTask(Ignite ignite, String cacheName, String sql,
        boolean loc, Object[] args, int fetchSize, UUID uuid, boolean locQry, boolean collocatedQry) {
        this.ignite = ignite;
        this.args = args;
        this.uuid = uuid;
        this.cacheName = cacheName;
        this.sql = sql;
        this.fetchSize = fetchSize;
        this.loc = loc;
        this.locQry = locQry;
        this.collocatedQry = collocatedQry;
    }

    /** {@inheritDoc} */
    @Override public JdbcQueryTask.QueryResult call() throws Exception {
        Cursor cursor = CURSORS.get(uuid);

        List<String> tbls = null;
        List<String> cols = null;
        List<String> types = null;

        boolean first;

        if (first = (cursor == null)) {
            IgniteCache<?, ?> cache = ignite.cache(cacheName);

            SqlFieldsQuery qry = new SqlFieldsQuery(sql).setArgs(args);

            qry.setPageSize(fetchSize);
            qry.setLocal(locQry);
            qry.setCollocated(collocatedQry);

            QueryCursor<List<?>> qryCursor = cache.query(qry);

            Collection<GridQueryFieldMetadata> meta = ((QueryCursorImpl<List<?>>)qryCursor).fieldsMeta();

            tbls = new ArrayList<>(meta.size());
            cols = new ArrayList<>(meta.size());
            types = new ArrayList<>(meta.size());

            for (GridQueryFieldMetadata desc : meta) {
                tbls.add(desc.typeName());
                cols.add(desc.fieldName().toUpperCase());
                types.add(desc.fieldTypeName());
            }

            CURSORS.put(uuid, cursor = new Cursor(qryCursor, qryCursor.iterator()));
        }

        List<List<?>> rows = new ArrayList<>();

        for (List<?> row : cursor) {
            List<Object> row0 = new ArrayList<>(row.size());

            for (Object val : row)
                row0.add(JdbcUtils.sqlType(val) ? val : val.toString());

            rows.add(row0);

            if (rows.size() == fetchSize) // If fetchSize is 0 then unlimited
                break;
        }

        boolean finished = !cursor.hasNext();

        if (finished)
            remove(uuid, cursor);
        else if (first) {
            if (!loc)
                scheduleRemoval(uuid, RMV_DELAY);
        }
        else if (!loc && !CURSORS.replace(uuid, cursor, new Cursor(cursor.cursor, cursor.iter)))
            assert !CURSORS.containsKey(uuid) : "Concurrent cursor modification.";

        return new QueryResult(uuid, finished, rows, cols, tbls, types);
    }

    /**
     * Schedules removal of stored cursor in case of remote query execution.
     *
     * @param uuid Cursor UUID.
     * @param delay Delay in milliseconds.
     */
    private void scheduleRemoval(final UUID uuid, long delay) {
        assert !loc;

        SCHEDULER.schedule(new CAX() {
            @Override public void applyx() {
                while (true) {
                    Cursor c = CURSORS.get(uuid);

                    if (c == null)
                        break;

                    // If the cursor was accessed since last scheduling then reschedule.
                    long untouchedTime = U.currentTimeMillis() - c.lastAccessTime;

                    if (untouchedTime < RMV_DELAY) {
                        scheduleRemoval(uuid, RMV_DELAY - untouchedTime);

                        break;
                    }
                    else if (remove(uuid, c))
                        break;
                }
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * @param uuid Cursor UUID.
     * @param c Cursor.
     * @return {@code true} If succeeded.
     */
    private static boolean remove(UUID uuid, Cursor c) {
        boolean rmv = CURSORS.remove(uuid, c);

        if (rmv)
            c.cursor.close();

        return rmv;
    }

    /**
     * Closes and removes cursor.
     *
     * @param uuid Cursor UUID.
     */
    static void remove(UUID uuid) {
        Cursor c = CURSORS.remove(uuid);

        if (c != null)
            c.cursor.close();
    }


    /**
     * Result of query execution.
     */
    static class QueryResult implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Uuid. */
        private final UUID uuid;

        /** Finished. */
        private final boolean finished;

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
         * @param rows Rows.
         * @param cols Columns.
         * @param tbls Tables.
         * @param types Types.
         */
        public QueryResult(UUID uuid, boolean finished, List<List<?>> rows, List<String> cols,
            List<String> tbls, List<String> types) {
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
    }

    /**
     * Cursor.
     */
    private static final class Cursor implements Iterable<List<?>> {
        /** Cursor. */
        final QueryCursor<List<?>> cursor;

        /** Iterator. */
        final Iterator<List<?>> iter;

        /** Last access time. */
        final long lastAccessTime;

        /**
         * @param cursor Cursor.
         * @param iter Iterator.
         */
        private Cursor(QueryCursor<List<?>> cursor, Iterator<List<?>> iter) {
            this.cursor = cursor;
            this.iter = iter;
            this.lastAccessTime = U.currentTimeMillis();
        }

        /** {@inheritDoc} */
        @Override public Iterator<List<?>> iterator() {
            return iter;
        }

        /**
         * @return {@code True} if cursor has next element.
         */
        public boolean hasNext() {
            return iter.hasNext();
        }
    }
}
