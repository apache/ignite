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

package org.apache.ignite.cache.store.cassandra.utils.common;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import java.util.regex.Pattern;

/**
 * Helper class providing methods to work with Cassandra session and exceptions
 */
public class CassandraHelper {
    /** TODO IGNITE-1371: add comment */
    private static final Pattern KEYSPACE_EXIST_ERROR1 = Pattern.compile("Keyspace [0-9a-zA-Z_]+ does not exist");

    /** TODO IGNITE-1371: add comment */
    private static final Pattern KEYSPACE_EXIST_ERROR2 = Pattern.compile("Cannot add table '[0-9a-zA-Z_]+' to non existing keyspace.*");

    /** TODO IGNITE-1371: add comment */
    private static final Pattern TABLE_EXIST_ERROR = Pattern.compile("unconfigured table [0-9a-zA-Z_]+");

    /** TODO IGNITE-1371: add comment */
    private static final String PREP_STATEMENT_CLUSTER_INSTANCE_ERROR = "You may have used a PreparedStatement that " +
        "was created with another Cluster instance";

    /** TODO IGNITE-1371: add comment */
    public static void closeSession(Session driverSes) {
        if (driverSes == null)
            return;

        Cluster cluster = driverSes.getCluster();

        try {
            if (!driverSes.isClosed())
                driverSes.close();
        }
        catch (Throwable ignored) {
        }

        try {
            if (!cluster.isClosed())
                cluster.close();
        }
        catch (Throwable ignored) {
        }
    }

    /** TODO IGNITE-1371: add comment */
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

    /** TODO IGNITE-1371: add comment */
    public static boolean isTableAbsenceError(Throwable e) {
        while (e != null) {
            if (e instanceof InvalidQueryException &&
                (TABLE_EXIST_ERROR.matcher(e.getMessage()).matches() ||
                    KEYSPACE_EXIST_ERROR1.matcher(e.getMessage()).matches() ||
                    KEYSPACE_EXIST_ERROR2.matcher(e.getMessage()).matches()))
                return true;

            e = e.getCause();
        }

        return false;
    }

    /** TODO IGNITE-1371: add comment */
    public static boolean isHostsAvailabilityError(Throwable e) {
        while (e != null) {
            if (e instanceof NoHostAvailableException ||
                e instanceof ReadTimeoutException)
                return true;

            e = e.getCause();
        }

        return false;
    }

    /** TODO IGNITE-1371: add comment */
    public static boolean isPreparedStatementClusterError(Throwable e) {
        while (e != null) {
            if (e instanceof InvalidQueryException && e.getMessage().contains(PREP_STATEMENT_CLUSTER_INSTANCE_ERROR))
                return true;

            e = e.getCause();
        }

        return false;
    }
}

