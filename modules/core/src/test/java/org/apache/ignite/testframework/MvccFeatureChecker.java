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

package org.apache.ignite.testframework;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.TRANSACTION_SERIALIZATION_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Provides checks for features supported when FORCE_MVCC mode is on.
 */
public class MvccFeatureChecker {
    /** */
    private static final boolean FORCE_MVCC =
        IgniteSystemProperties.getBoolean(IGNITE_FORCE_MVCC_MODE_IN_TESTS, false);

    /** */
    public enum Feature {
        CACHE_STORE,
        NEAR_CACHE,
        LOCAL_CACHE,
        ENTRY_LOCK,
        CACHE_EVENTS,
        EVICTION,
        EXPIRATION
    }

    /**
     * Fails if feature is not supported.
     *
     * @param f feature.
     * @throws AssertionError If failed.
     */
    public static void failIfNotSupported(Feature f) {
        if (!forcedMvcc())
            return;

        validateFeature(f);
    }

    /**
     * @return {@code True} if Mvcc mode is forced.
     */
    public static boolean forcedMvcc() {
        return FORCE_MVCC;
    }

    /**
     * Check if feature is supported.
     *
     * @param f Feature.
     * @return {@code True} if feature is supported, {@code False} otherwise.
     */
    public static boolean isSupported(Feature f) {
        try {
            validateFeature(f);

            return true;
        }
        catch (AssertionError ignore) {
            return false;
        }
    }

    /**
     * Check if Tx mode is supported.
     *
     * @param conc Transaction concurrency.
     * @param iso Transaction isolation.
     * @return {@code True} if feature is supported, {@code False} otherwise.
     */
    public static boolean isSupported(TransactionConcurrency conc, TransactionIsolation iso) {
            return conc == TransactionConcurrency.PESSIMISTIC &&
                iso == TransactionIsolation.REPEATABLE_READ;
    }


    /**
     * Check if Cache mode is supported.
     *
     * @param mode Cache mode.
     * @return {@code True} if feature is supported, {@code False} otherwise.
     */
    public static boolean isSupported(CacheMode mode) {
        return mode != CacheMode.LOCAL || isSupported(Feature.LOCAL_CACHE);
    }

    /**
     * Checks if given exception was caused by MVCC write conflict.
     *
     * @param e Exception.
     */
    public static void assertMvccWriteConflict(Exception e) {
        IgniteSQLException sqlEx = X.cause(e, IgniteSQLException.class);

        assertNotNull(sqlEx);

        assertEquals(TRANSACTION_SERIALIZATION_ERROR, sqlEx.statusCode());
    }

    /**
     * Fails if feature is not supported in Mvcc mode.
     *
     * @param feature Mvcc feature.
     * @throws AssertionError If failed.
     */
    private static void validateFeature(Feature feature) {
        switch (feature) {
            case NEAR_CACHE:
                fail("https://issues.apache.org/jira/browse/IGNITE-7187");

            case LOCAL_CACHE:
                fail("https://issues.apache.org/jira/browse/IGNITE-9530");

            case CACHE_STORE:
                fail("https://issues.apache.org/jira/browse/IGNITE-8582");

            case ENTRY_LOCK:
                fail("https://issues.apache.org/jira/browse/IGNITE-9324");

            case CACHE_EVENTS:
                fail("https://issues.apache.org/jira/browse/IGNITE-9321");

            case EVICTION:
                fail("https://issues.apache.org/jira/browse/IGNITE-7956");

            case EXPIRATION:
                fail("https://issues.apache.org/jira/browse/IGNITE-7311");
        }
    }
}
