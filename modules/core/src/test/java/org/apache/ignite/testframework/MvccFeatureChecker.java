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

import javax.cache.CacheException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionSerializationException;
import org.junit.Assume;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS;
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
        EXPIRATION,
        METRICS,
        INTERCEPTOR
    }

    /**
     * Fails if feature is not supported.
     *
     * @param f feature.
     * @throws AssertionError If failed.
     * @deprecated Use {@link #skipIfNotSupported(Feature)} instead.
     */
    @Deprecated
    public static void failIfNotSupported(Feature f) {
        if (!forcedMvcc())
            return;

        String reason = unsupportedReason(f);

        if (reason != null)
            fail(reason);
    }

    /**
     * Skips test if feature is not supported.
     *
     * @param f feature.
     */
    public static void skipIfNotSupported(Feature f) {
        if (!forcedMvcc())
            return;

        String reason = unsupportedReason(f);

        Assume.assumeTrue(reason, reason == null);
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
        return unsupportedReason(f) == null;
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
        assert e != null;

        if (e instanceof CacheException && e.getCause() instanceof TransactionSerializationException)
            return;

        fail("Unexpected exception: " + X.getFullStackTrace(e));
    }

    /**
     * Fails if feature is not supported in Mvcc mode.
     *
     * @param feature Mvcc feature.
     * @throws AssertionError If failed.
     */
    private static String unsupportedReason(Feature feature) {
        switch (feature) {
            case NEAR_CACHE:
                return "https://issues.apache.org/jira/browse/IGNITE-7187";

            case LOCAL_CACHE:
                return "https://issues.apache.org/jira/browse/IGNITE-9530";

            case CACHE_STORE:
                return "https://issues.apache.org/jira/browse/IGNITE-8582";

            case ENTRY_LOCK:
                return "https://issues.apache.org/jira/browse/IGNITE-9324";

            case CACHE_EVENTS:
                return "https://issues.apache.org/jira/browse/IGNITE-9321";

            case EVICTION:
                return "https://issues.apache.org/jira/browse/IGNITE-7956";

            case EXPIRATION:
                return "https://issues.apache.org/jira/browse/IGNITE-7311";

            case METRICS:
                return "https://issues.apache.org/jira/browse/IGNITE-9224";

            case INTERCEPTOR:
                return "https://issues.apache.org/jira/browse/IGNITE-9323";
        }

        return null;
    }
}
