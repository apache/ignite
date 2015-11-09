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
    private static final int CQL_EXECUTION_ATTEMPTS_COUNT = 20;
    private static final int CQL_EXECUTION_ATTEMPTS_TIMEOUT = 2000;

    private volatile Cluster.Builder builder;
    private volatile Session session;
    private volatile int refCount = 0;

    private Integer fetchSize;
    private ConsistencyLevel readConsistency;
    private ConsistencyLevel writeConsistency;
    private IgniteLogger logger;

    private final AtomicInteger handlersCount = new AtomicInteger(-1);

    public CassandraSessionImpl(Cluster.Builder builder, Integer fetchSize, ConsistencyLevel readConsistency,
        ConsistencyLevel writeConsistency, IgniteLogger logger) {
        this.builder = builder;
        this.fetchSize = fetchSize;
        this.readConsistency = readConsistency;
        this.writeConsistency = writeConsistency;
        this.logger = logger;
    }

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
                    ResultSet result = session().execute(statement);

                    Row row = result == null || !result.iterator().hasNext() ? null : result.iterator().next();

                    return row == null ? null : assistant.process(row);
                }
                catch (Throwable e) {
                    if (CassandraHelper.isTableAbsenceError(e)) {
                        if (!assistant.tableExistenceRequired()) {
                            logger.warning(errorMsg, e);
                            return null;
                        }

                        handleTableAbsenceError(assistant.getPersistenceSettings());
                    }
                    else if (CassandraHelper.isHostsAvailabilityError(e))
                        handleHostsAvailabilityError(e, attempt, errorMsg);
                    else if (!CassandraHelper.isPreparedStatementClusterError(e))
                        throw new IgniteException(errorMsg, e);

                    if (logger != null && !CassandraHelper.isPreparedStatementClusterError(e))
                        logger.warning(errorMsg, e);

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
                boolean tableAbsenceErrorFlag = false;
                boolean hostsAvailabilityErrorFlag = false;
                boolean prepStatementErrorFlag = false;
                error = null;

                List<Cache.Entry<Integer, ResultSetFuture>> futureResults = new LinkedList<>();

                PreparedStatement preparedSt = prepareStatement(assistant.getStatement(),
                    assistant.getPersistenceSettings(), assistant.tableExistenceRequired());

                if (preparedSt == null)
                    return null;

                int sequenceNumber = 0;

                for (V obj : data) {
                    if (assistant.alreadyProcessed(sequenceNumber))
                        continue;

                    Statement statement = tuneStatementExecutionOptions(assistant.bindStatement(preparedSt, obj));
                    ResultSetFuture future = session().executeAsync(statement);
                    futureResults.add(new CacheEntryImpl<>(sequenceNumber, future));

                    sequenceNumber++;
                }

                dataSize = sequenceNumber;

                for (Cache.Entry<Integer, ResultSetFuture> futureResult : futureResults) {
                    try {
                        ResultSet resultSet = futureResult.getValue().getUninterruptibly();
                        Row row = resultSet != null && resultSet.iterator().hasNext() ? resultSet.iterator().next() : null;

                        if (row != null)
                            assistant.process(row, futureResult.getKey());
                    }
                    catch (Throwable e) {
                        if (CassandraHelper.isTableAbsenceError(e))
                            tableAbsenceErrorFlag = true;
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
                if (!tableAbsenceErrorFlag && !hostsAvailabilityErrorFlag && !prepStatementErrorFlag)
                    throw new IgniteException(errorMsg, error);

                if (logger != null && !CassandraHelper.isPreparedStatementClusterError(error))
                    logger.warning(errorMsg, error);

                // if there are only table absence errors and it is not required for the operation we can return
                if (tableAbsenceErrorFlag && !assistant.tableExistenceRequired())
                    return assistant.processedData();

                if (tableAbsenceErrorFlag)
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

        if (logger != null)
            logger.warning(errorMsg, error);

        return assistant.processedData();
    }

    @Override public void execute(BatchLoaderAssistant assistant) {
        int attempt = 0;
        String errorMsg = "Failed to execute Cassandra " + assistant.operationName() + " operation";
        Throwable error = null;

        incrementSessionRefs();

        try {
            while (attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
                Statement statement = tuneStatementExecutionOptions(assistant.getStatement());
                ResultSetFuture future = session().executeAsync(statement);

                try {
                    ResultSet resultSet = future.getUninterruptibly();
                    if (resultSet == null || !resultSet.iterator().hasNext())
                        return;

                    for (Row row : resultSet)
                        assistant.process(row);

                    return;
                }
                catch (Throwable e) {
                    if (!CassandraHelper.isTableAbsenceError(e) && !CassandraHelper.isHostsAvailabilityError(e))
                        throw new IgniteException(errorMsg, e);

                    if (logger != null)
                        logger.warning(errorMsg, e);

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

    @Override public synchronized void close() throws IOException {
        if (decrementSessionRefs() == 0 && session != null) {
            SessionPool.put(this, session);
            session = null;
        }
    }

    private synchronized void refresh() {
        //make sure that session removed from the pool
        SessionPool.get(this);

        //closing and reopening session
        CassandraHelper.closeSession(session);
        session = null;
        session();
    }

    private synchronized Session session() {
        if (session != null)
            return session;

        session = SessionPool.get(this);

        if (session != null)
            return session;

        try {
            return session = builder.build().connect();
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to establish session with Cassandra database", e);
        }
    }

    private synchronized void incrementSessionRefs() {
        refCount++;
    }

    private synchronized int decrementSessionRefs() {
        if (refCount != 0)
            refCount--;

        return refCount;
    }

    private PreparedStatement prepareStatement(String statement, KeyValuePersistenceSettings settings,
        boolean tableExistenceRequired) {

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
                        if (!tableExistenceRequired)
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

    private void createKeyspace(KeyValuePersistenceSettings settings) {
        int attempt = 0;
        Throwable error = null;
        String errorMsg = "Failed to create Cassandra keyspace '" + settings.getKeyspace() + "'";

        while (attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
            try {
                logger.info("Creating Cassandra keyspace '" + settings.getKeyspace() + "'");
                session().execute(settings.getKeyspaceDDLStatement());
                logger.info("Cassandra keyspace '" + settings.getKeyspace() + "' was successfully created");
                return;
            }
            catch (AlreadyExistsException ignored) {
                logger.info("Cassandra keyspace '" + settings.getKeyspace() + "' already exist");
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

    private void createTable(KeyValuePersistenceSettings settings) {
        int attempt = 0;
        Throwable error = null;
        String errorMsg = "Failed to create Cassandra table '" + settings.getTableFullName() + "'";

        while (attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
            try {
                logger.info("Creating Cassandra table '" + settings.getTableFullName() + "'");
                session().execute(settings.getTableDDLStatement());
                logger.info("Cassandra table '" + settings.getTableFullName() + "' was successfully created");
                return;
            }
            catch (AlreadyExistsException ignored) {
                logger.info("Cassandra table '" + settings.getTableFullName() + "' already exist");
                return;
            }
            catch (Throwable e) {
                if (!CassandraHelper.isHostsAvailabilityError(e) && !CassandraHelper.isKeyspaceAbsenceError(e))
                    throw new IgniteException(errorMsg, e);

                if (CassandraHelper.isKeyspaceAbsenceError(e)) {
                    logger.warning("Failed to create Cassandra table '" + settings.getTableFullName() +
                        "' cause appropriate keyspace doesn't exist", e);
                    createKeyspace(settings);
                }
                else if (CassandraHelper.isHostsAvailabilityError(e)) {
                    handleHostsAvailabilityError(e, attempt, errorMsg);
                }

                error = e;
            }

            attempt++;
        }

        throw new IgniteException(errorMsg, error);
    }

    private void createTableIndexes(KeyValuePersistenceSettings settings) {
        if (settings.getIndexDDLStatements() == null || settings.getIndexDDLStatements().isEmpty())
            return;

        int attempt = 0;
        Throwable error = null;
        String errorMsg = "Failed to create indexes for Cassandra table " + settings.getTableFullName();

        while (attempt < CQL_EXECUTION_ATTEMPTS_COUNT) {
            try {
                logger.info("Creating indexes for Cassandra table '" + settings.getTableFullName() + "'");

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

                logger.info("Indexes for Cassandra table '" + settings.getTableFullName() + "' were successfully created");

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

    private Statement tuneStatementExecutionOptions(Statement statement) {
        String query = "";

        if (statement instanceof BoundStatement)
            query = ((BoundStatement)statement).preparedStatement().getQueryString().trim().toLowerCase();
        else if (statement instanceof PreparedStatement)
            query = ((PreparedStatement)statement).getQueryString().trim().toLowerCase();

        boolean readStatement = query.startsWith("select");
        boolean writeStatement = statement instanceof Batch || statement instanceof BatchStatement ||
            query.startsWith("insert") || query.startsWith("delete") || query.startsWith("update");

        if (readStatement && readConsistency != null)
            statement.setConsistencyLevel(readConsistency);

        if (writeStatement && writeConsistency != null)
            statement.setConsistencyLevel(writeConsistency);

        if (fetchSize != null)
            statement.setFetchSize(fetchSize);

        return statement;
    }

    private void handleTableAbsenceError(KeyValuePersistenceSettings settings) {
        int handlerNumber = handlersCount.incrementAndGet();

        try {
            synchronized (handlersCount) {
                // Oooops... I am not the first thread who tried to handle table absence problem
                if (handlerNumber != 0)
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
            if (handlerNumber == 0)
                handlersCount.set(-1);
        }
    }

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
