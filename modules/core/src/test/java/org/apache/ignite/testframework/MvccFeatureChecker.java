package org.apache.ignite.testframework;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS;
import static org.junit.Assert.fail;

/**
 * Provides checks for features supported when FORCE_MVCC mode is on.
 */
public class MvccFeatureChecker {
    /** */
    public static final boolean FORCE_MVCC =
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
        if (!FORCE_MVCC)
            return;

        validateFeature(f);
    }

    /**
     * Check if feature is supported.
     *
     * @param f Feature.
     * @return {@code True} if feature is supported, {@code False} otherwise.
     */
    public static boolean isSupported(Feature f) {
        if (!FORCE_MVCC)
            return true;

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
