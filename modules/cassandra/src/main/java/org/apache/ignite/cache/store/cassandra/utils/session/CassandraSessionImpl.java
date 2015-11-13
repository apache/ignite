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

package org.apache.ignite.cache.store.cassandra.utils.session;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Batch;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.cassandra.utils.common.CassandraHelper;
import org.apache.ignite.cache.store.cassandra.utils.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.cache.store.cassandra.utils.session.pool.SessionPool;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;

/**
 * Implementation for ${@link org.apache.ignite.cache.store.cassandra.utils.session.CassandraSession}
 */
public class CassandraSessionImpl implements CassandraSession {
    /** TODO IGNITE-1371: add comment */
    private static final int CQL_EXECUTION_ATTEMPTS_COUNT = 20;

    /** TODO IGNITE-1371: add comment */
    private static final int CQL_EXECUTION_ATTEMPTS_TIMEOUT = 2000;

    /** TODO IGNITE-1371: add comment */
    private volatile Cluster.Builder builder;

    /** TODO IGNITE-1371: add comment */
    private volatile Session ses;

    /** TODO IGNITE-1371: add comment */
    private volatile int refCnt = 0;

    /** TODO IGNITE-1371: add comment */
    private Integer fetchSize;

    /** TODO IGNITE-1371: add comment */
    private ConsistencyLevel readConsistency;

    /** TODO IGNITE-1371: add comment */
    private ConsistencyLevel writeConsistency;

    /** TODO IGNITE-1371: add comment */
    private IgniteLogger log;

    /** TODO IGNITE-1371: add comment */
    private final AtomicInteger handlersCnt = new AtomicInteger(-1);

    /** TODO IGNITE-1371: add comment */
    public CassandraSessionImpl(Cluster.Builder builder, Integer fetchSize, ConsistencyLevel readConsistency,
        ConsistencyLevel writeConsistency, IgniteLogger log) {
        this.builder = builder;
        this.fetchSize = fetchSize;
        this.readConsistency = readConsistency;
        this.writeConsistency = writeConsistency;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public <V> V execute(ExecutionAssistant<V> assistant) {
        int attempt = 0;
        Throwable error = null;
        String errorMsg = "Failed to execute Cassandra CQL statement: " + assistant.getStatement();

        incrementSessionRefs();

        try {
            while (attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
                error = null;

                try {
                    PreparedStatement preparedSt = prepareStatement(assistant.getStatement(),
                        assistant.getPersistenceSettings(), assistant.tableExistenceRequired());

                    if (preparedSt == null)
                        return null;

                    Statement statement = tuneStatementExecutionOptions(assistant.bindStatement(preparedSt));
                    ResultSet res = session().execute(statement);

                    Row row = res == null || !res.iterator().hasNext() ? null : res.iterator().next();

                    return row == null ? null : assistant.process(row);
                }
                catch (Throwable e) {
                    if (CassandraHelper.isTableAbsenceError(e)) {
                        if (!assistant.tableExistenceRequired()) {
                            log.warning(errorMsg, e);
                            return null;
                        }

                        handleTableAbsenceError(assistant.getPersistenceSettings());
                    }
                    else if (CassandraHelper.isHostsAvailabilityError(e))
                        handleHostsAvailabilityError(e, attempt, errorMsg);
                    else if (!CassandraHelper.isPreparedStatementClusterError(e))
                        throw new IgniteException(errorMsg, e);

                    if (log != null && !CassandraHelper.isPreparedStatementClusterError(e))
                        log.warning(errorMsg, e);

                    error = e;
                }

                attempt++;
            }
        }
        finally {
            decrementSessionRefs();
        }

        throw new IgniteException("Failed to execute Cassandra CQL statement: " + assistant.getStatement(), error);
    }

    /** {@inheritDoc} */
    @Override public <R, V> R execute(BatchExecutionAssistant<R, V> assistant, Iterable<? extends V> data) {
        if (data == null || !data.iterator().hasNext())
            return assistant.processedData();

        int attempt = 0;
        String errorMsg = "Failed to execute Cassandra " + assistant.operationName() + " operation";
        Throwable error = new IgniteException(errorMsg);

        int dataSize = -1;

        incrementSessionRefs();

        try {
            while (dataSize != assistant.processedCount() && error != null && attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
                boolean tblAbsenceErrorFlag = false;
                boolean hostsAvailabilityErrorFlag = false;
                boolean prepStatementErrorFlag = false;
                error = null;

                List<Cache.Entry<Integer, ResultSetFuture>> futResults = new LinkedList<>();

                PreparedStatement preparedSt = prepareStatement(assistant.getStatement(),
                    assistant.getPersistenceSettings(), assistant.tableExistenceRequired());

                if (preparedSt == null)
                    return null;

                int seqNum = 0;

                for (V obj : data) {
                    if (assistant.alreadyProcessed(seqNum))
                        continue;

                    Statement statement = tuneStatementExecutionOptions(assistant.bindStatement(preparedSt, obj));
                    ResultSetFuture fut = session().executeAsync(statement);
                    futResults.add(new CacheEntryImpl<>(seqNum, fut));

                    seqNum++;
                }

                dataSize = seqNum;

                for (Cache.Entry<Integer, ResultSetFuture> futureResult : futResults) {
                    try {
                        ResultSet resSet = futureResult.getValue().getUninterruptibly();
                        Row row = resSet != null && resSet.iterator().hasNext() ? resSet.iterator().next() : null;

                        if (row != null)
                            assistant.process(row, futureResult.getKey());
                    }
                    catch (Throwable e) {
                        if (CassandraHelper.isTableAbsenceError(e))
                            tblAbsenceErrorFlag = true;
                        else if (CassandraHelper.isHostsAvailabilityError(e))
                            hostsAvailabilityErrorFlag = true;
                        else if (CassandraHelper.isPreparedStatementClusterError(e))
                            prepStatementErrorFlag = true;

                        error = error == null || !CassandraHelper.isPreparedStatementClusterError(e) ? e : error;
                    }
                }

                // if no errors occurred it means that operation successfully completed and we can return
                if (error == null)
                    return assistant.processedData();

                // if there were no errors which we know how to handle, we will not try next attempts and terminate
                if (!tblAbsenceErrorFlag && !hostsAvailabilityErrorFlag && !prepStatementErrorFlag)
                    throw new IgniteException(errorMsg, error);

                if (log != null && !CassandraHelper.isPreparedStatementClusterError(error))
                    log.warning(errorMsg, error);

                // if there are only table absence errors and it is not required for the operation we can return
                if (tblAbsenceErrorFlag && !assistant.tableExistenceRequired())
                    return assistant.processedData();

                if (tblAbsenceErrorFlag)
                    handleTableAbsenceError(assistant.getPersistenceSettings());

                if (hostsAvailabilityErrorFlag)
                    handleHostsAvailabilityError(error, attempt, errorMsg);

                attempt++;
            }
        }
        finally {
            decrementSessionRefs();
        }

        errorMsg = "Failed to process " + (dataSize - assistant.processedCount()) +
            " of " + dataSize + " elements during " + assistant.operationName() +
            " operation with Cassandra";

        if (assistant.processedCount() == 0)
            throw new IgniteException(errorMsg, error);

        if (log != null)
            log.warning(errorMsg, error);

        return assistant.processedData();
    }

    /** {@inheritDoc} */
    @Override public void execute(BatchLoaderAssistant assistant) {
        int attempt = 0;
        String errorMsg = "Failed to execute Cassandra " + assistant.operationName() + " operation";
        Throwable error = null;

        incrementSessionRefs();

        try {
            while (attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
                Statement statement = tuneStatementExecutionOptions(assistant.getStatement());
                ResultSetFuture fut = session().executeAsync(statement);

                try {
                    ResultSet resSet = fut.getUninterruptibly();
                    if (resSet == null || !resSet.iterator().hasNext())
                        return;

                    for (Row row : resSet)
                        assistant.process(row);

                    return;
                }
                catch (Throwable e) {
                    if (!CassandraHelper.isTableAbsenceError(e) && !CassandraHelper.isHostsAvailabilityError(e))
                        throw new IgniteException(errorMsg, e);

                    if (log != null)
                        log.warning(errorMsg, e);

                    if (CassandraHelper.isTableAbsenceError(e))
                        return;

                    if (CassandraHelper.isHostsAvailabilityError(e))
                        handleHostsAvailabilityError(e, attempt, errorMsg);

                    error = e;
                }

                attempt++;
            }
        }
        finally {
            decrementSessionRefs();
        }

        throw new IgniteException(errorMsg, error);
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() throws IOException {
        if (decrementSessionRefs() == 0 && ses != null) {
            SessionPool.put(this, ses);
            ses = null;
        }
    }

    /** TODO IGNITE-1371: add comment */
    private synchronized void refresh() {
        //make sure that session removed from the pool
        SessionPool.get(this);

        //closing and reopening session
        CassandraHelper.closeSession(ses);
        ses = null;
        session();
    }

    /** TODO IGNITE-1371: add comment */
    private synchronized Session session() {
        if (ses != null)
            return ses;

        ses = SessionPool.get(this);

        if (ses != null)
            return ses;

        try {
            return ses = builder.build().connect();
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to establish session with Cassandra database", e);
        }
    }

    /** TODO IGNITE-1371: add comment */
    private synchronized void incrementSessionRefs() {
        refCnt++;
    }

    /** TODO IGNITE-1371: add comment */
    private synchronized int decrementSessionRefs() {
        if (refCnt != 0)
            refCnt--;

        return refCnt;
    }

    /** TODO IGNITE-1371: add comment */
    private PreparedStatement prepareStatement(String statement, KeyValuePersistenceSettings settings,
        boolean tblExistenceRequired) {

        int attempt = 0;
        Throwable error = null;
        String errorMsg = "Failed to prepare Cassandra CQL statement: " + statement;

        incrementSessionRefs();

        try {
            while (attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
                try {
                    return session().prepare(statement);
                }
                catch (Throwable e) {
                    if (CassandraHelper.isTableAbsenceError(e)) {
                        if (!tblExistenceRequired)
                            return null;

                        handleTableAbsenceError(settings);
                    }
                    else if (CassandraHelper.isHostsAvailabilityError(e))
                        handleHostsAvailabilityError(e, attempt, errorMsg);
                    else
                        throw new IgniteException(errorMsg, e);

                    error = e;
                }

                attempt++;
            }
        }
        finally {
            decrementSessionRefs();
        }

        throw new IgniteException(errorMsg, error);
    }

    /** TODO IGNITE-1371: add comment */
    private void createKeyspace(KeyValuePersistenceSettings settings) {
        int attempt = 0;
        Throwable error = null;
        String errorMsg = "Failed to create Cassandra keyspace '" + settings.getKeyspace() + "'";

        while (attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
            try {
                log.info("Creating Cassandra keyspace '" + settings.getKeyspace() + "'");
                session().execute(settings.getKeyspaceDDLStatement());
                log.info("Cassandra keyspace '" + settings.getKeyspace() + "' was successfully created");
                return;
            }
            catch (AlreadyExistsException ignored) {
                log.info("Cassandra keyspace '" + settings.getKeyspace() + "' already exist");
                return;
            }
            catch (Throwable e) {
                if (!CassandraHelper.isHostsAvailabilityError(e))
                    throw new IgniteException(errorMsg, e);

                handleHostsAvailabilityError(e, attempt, errorMsg);

                error = e;
            }

            attempt++;
        }

        throw new IgniteException(errorMsg, error);
    }

    /** TODO IGNITE-1371: add comment */
    private void createTable(KeyValuePersistenceSettings settings) {
        int attempt = 0;
        Throwable error = null;
        String errorMsg = "Failed to create Cassandra table '" + settings.getTableFullName() + "'";

        while (attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
            try {
                log.info("Creating Cassandra table '" + settings.getTableFullName() + "'");
                session().execute(settings.getTableDDLStatement());
                log.info("Cassandra table '" + settings.getTableFullName() + "' was successfully created");
                return;
            }
            catch (AlreadyExistsException ignored) {
                log.info("Cassandra table '" + settings.getTableFullName() + "' already exist");
                return;
            }
            catch (Throwable e) {
                if (!CassandraHelper.isHostsAvailabilityError(e) && !CassandraHelper.isKeyspaceAbsenceError(e))
                    throw new IgniteException(errorMsg, e);

                if (CassandraHelper.isKeyspaceAbsenceError(e)) {
                    log.warning("Failed to create Cassandra table '" + settings.getTableFullName() +
                        "' cause appropriate keyspace doesn't exist", e);
                    createKeyspace(settings);
                }
                else if (CassandraHelper.isHostsAvailabilityError(e))
                    handleHostsAvailabilityError(e, attempt, errorMsg);

                error = e;
            }

            attempt++;
        }

        throw new IgniteException(errorMsg, error);
    }

    /** TODO IGNITE-1371: add comment */
    private void createTableIndexes(KeyValuePersistenceSettings settings) {
        if (settings.getIndexDDLStatements() == null || settings.getIndexDDLStatements().isEmpty())
            return;

        int attempt = 0;
        Throwable error = null;
        String errorMsg = "Failed to create indexes for Cassandra table " + settings.getTableFullName();

        while (attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
            try {
                log.info("Creating indexes for Cassandra table '" + settings.getTableFullName() + "'");

                for (String statement : settings.getIndexDDLStatements()) {
                    try {
                        session().execute(statement);
                    }
                    catch (AlreadyExistsException ignored) {
                    }
                    catch (Throwable e) {
                        if (!(e instanceof InvalidQueryException) || !e.getMessage().equals("Index already exists"))
                            throw new IgniteException(errorMsg, e);
                    }
                }

                log.info("Indexes for Cassandra table '" + settings.getTableFullName() + "' were successfully created");

                return;
            }
            catch (Throwable e) {
                if (CassandraHelper.isHostsAvailabilityError(e))
                    handleHostsAvailabilityError(e, attempt, errorMsg);
                else if (CassandraHelper.isTableAbsenceError(e))
                    createTable(settings);
                else
                    throw new IgniteException(errorMsg, e);

                error = e;
            }

            attempt++;
        }

        throw new IgniteException(errorMsg, error);
    }

    /** TODO IGNITE-1371: add comment */
    private Statement tuneStatementExecutionOptions(Statement statement) {
        String qry = "";

        if (statement instanceof BoundStatement)
            qry = ((BoundStatement)statement).preparedStatement().getQueryString().trim().toLowerCase();
        else if (statement instanceof PreparedStatement)
            qry = ((PreparedStatement)statement).getQueryString().trim().toLowerCase();

        boolean readStatement = qry.startsWith("select");
        boolean writeStatement = statement instanceof Batch || statement instanceof BatchStatement ||
            qry.startsWith("insert") || qry.startsWith("delete") || qry.startsWith("update");

        if (readStatement && readConsistency != null)
            statement.setConsistencyLevel(readConsistency);

        if (writeStatement && writeConsistency != null)
            statement.setConsistencyLevel(writeConsistency);

        if (fetchSize != null)
            statement.setFetchSize(fetchSize);

        return statement;
    }

    /** TODO IGNITE-1371: add comment */
    private void handleTableAbsenceError(KeyValuePersistenceSettings settings) {
        int hndNum = handlersCnt.incrementAndGet();

        try {
            synchronized (handlersCnt) {
                // Oooops... I am not the first thread who tried to handle table absence problem
                if (hndNum != 0)
                    return;

                RuntimeException error = new IgniteException("Failed to create Cassandra table " + settings.getTableFullName());

                int attempt = 0;

                while (error != null && attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
                    error = null;

                    try {
                        createKeyspace(settings);
                        createTable(settings);
                        createTableIndexes(settings);
                    }
                    catch (Throwable e) {
                        if (CassandraHelper.isHostsAvailabilityError(e))
                            handleHostsAvailabilityError(e, attempt, null);
                        else
                            throw new IgniteException("Failed to create Cassandra table " + settings.getTableFullName(), e);

                        error = e instanceof RuntimeException ? (RuntimeException)e : new IgniteException(e);
                    }

                    attempt++;
                }

                if (error != null)
                    throw error;
            }
        }
        finally {
            if (hndNum == 0)
                handlersCnt.set(-1);
        }
    }

    /** TODO IGNITE-1371: add comment */
    private boolean handleHostsAvailabilityError(Throwable e, int attempt, String msg) {
        if (attempt >= CQL_EXECUTION_ATTEMPTS_COUNT)
            throw msg == null ? new IgniteException(e) : new IgniteException(msg, e);

        if (attempt == CQL_EXECUTION_ATTEMPTS_COUNT / 4  ||
            attempt == CQL_EXECUTION_ATTEMPTS_COUNT / 3  ||
            attempt == CQL_EXECUTION_ATTEMPTS_COUNT / 2  ||
            attempt == CQL_EXECUTION_ATTEMPTS_COUNT - 1) {
            refresh();
            return true;
        }

        try {
            Thread.sleep(CQL_EXECUTION_ATTEMPTS_TIMEOUT);
        }
        catch (InterruptedException ex) {
            throw new IgniteException(msg, e);
        }

        return false;
    }
}
