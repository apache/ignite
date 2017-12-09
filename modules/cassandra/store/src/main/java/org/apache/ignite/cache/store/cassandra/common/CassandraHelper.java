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

package org.apache.ignite.cache.store.cassandra.common;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.ignite.cache.store.cassandra.handler.TypeHandler;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Helper class providing methods to work with Cassandra session and exceptions
 */
public class CassandraHelper {
    /** Cassandra error message if specified keyspace doesn't exist. */
    private static final Pattern KEYSPACE_EXIST_ERROR1 = Pattern.compile("Keyspace [0-9a-zA-Z_]+ does not exist");

    /** Cassandra error message if trying to create table inside nonexistent keyspace. */
    private static final Pattern KEYSPACE_EXIST_ERROR2 = Pattern.compile("Cannot add table '[0-9a-zA-Z_]+' to non existing keyspace.*");

    /** Cassandra error message if trying to create table inside nonexistent keyspace. */
    private static final Pattern KEYSPACE_EXIST_ERROR3 = Pattern.compile("Error preparing query, got ERROR INVALID: " +
            "Keyspace [0-9a-zA-Z_]+ does not exist");

    /** Cassandra error message if specified table doesn't exist. */
    private static final Pattern TABLE_EXIST_ERROR1 = Pattern.compile("unconfigured table [0-9a-zA-Z_]+");

    /** Cassandra error message if specified table doesn't exist. */
    private static final String TABLE_EXIST_ERROR2 = "Error preparing query, got ERROR INVALID: unconfigured table";

    /** Cassandra error message if trying to use prepared statement created from another session. */
    private static final String PREP_STATEMENT_CLUSTER_INSTANCE_ERROR = "You may have used a PreparedStatement that " +
        "was created with another Cluster instance";

    /** Closes Cassandra driver session. */
    public static void closeSession(Session driverSes) {
        if (driverSes == null)
            return;

        Cluster cluster = driverSes.getCluster();

        if (!driverSes.isClosed())
            U.closeQuiet(driverSes);

        if (!cluster.isClosed())
            U.closeQuiet(cluster);
    }

    /**
     * Checks if Cassandra keyspace absence error occur.
     *
     * @param e Exception to check.
     * @return {@code true} in case of keyspace absence error.
     */
    public static boolean isKeyspaceAbsenceError(Throwable e) {
        while (e != null) {
            if (e instanceof InvalidQueryException &&
                (KEYSPACE_EXIST_ERROR1.matcher(e.getMessage()).matches() ||
                    KEYSPACE_EXIST_ERROR2.matcher(e.getMessage()).matches()))
                return true;

            e = e.getCause();
        }

        return false;
    }

    /**
     * Checks if Cassandra table absence error occur.
     *
     * @param e Exception to check.
     * @return {@code true} in case of table absence error.
     */
    public static boolean isTableAbsenceError(Throwable e) {
        while (e != null) {
            if (e instanceof InvalidQueryException &&
                (TABLE_EXIST_ERROR1.matcher(e.getMessage()).matches() ||
                    KEYSPACE_EXIST_ERROR1.matcher(e.getMessage()).matches() ||
                    KEYSPACE_EXIST_ERROR2.matcher(e.getMessage()).matches()))
                return true;

            if (e instanceof NoHostAvailableException && ((NoHostAvailableException) e).getErrors() != null) {
                NoHostAvailableException ex = (NoHostAvailableException)e;

                for (Map.Entry<InetSocketAddress, Throwable> entry : ex.getErrors().entrySet()) {
                    //noinspection ThrowableResultOfMethodCallIgnored
                    Throwable error = entry.getValue();

                    if (error instanceof DriverException &&
                        (error.getMessage().contains(TABLE_EXIST_ERROR2) ||
                             KEYSPACE_EXIST_ERROR3.matcher(error.getMessage()).matches()))
                        return true;
                }
            }

            e = e.getCause();
        }

        return false;
    }

    /**
     * Checks if Cassandra host availability error occur, thus host became unavailable.
     *
     * @param e Exception to check.
     * @return {@code true} in case of host not available error.
     */
    public static boolean isHostsAvailabilityError(Throwable e) {
        while (e != null) {
            if (e instanceof NoHostAvailableException ||
                e instanceof ReadTimeoutException)
                return true;

            e = e.getCause();
        }

        return false;
    }

    /**
     * Checks if Cassandra error occur because of prepared statement created in one session was used in another session.
     *
     * @param e Exception to check.
     * @return {@code true} in case of invalid usage of prepared statement.
     */
    public static boolean isPreparedStatementClusterError(Throwable e) {
        while (e != null) {
            if (e instanceof InvalidQueryException && e.getMessage().contains(PREP_STATEMENT_CLUSTER_INSTANCE_ERROR))
                return true;

            e = e.getCause();
        }

        return false;
    }

    /**
     * Checks if two Type Handlers are Cassandra compatible - mapped to the same Cassandra type.
     *
     * @param type1 First handler.
     * @param type2 Second handler.
     * @return {@code true} if cassandra types are compatible and {@code false} if not.
     */
    public static boolean isCassandraCompatibleTypes(TypeHandler type1, TypeHandler type2) {
        if (type1 == null || type2 == null)
            return false;

        String t1 = type1.getDDLType();
        String t2 = type2.getDDLType();

        return t1.equals(t2);
    }
}

