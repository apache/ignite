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

/**
 * Provides checks for features supported when FORCE_MVCC mode is on.
 */
public class MvccFeatureChecker {
    /** */
    public enum Feature {
        /** */
        CACHE_STORE,

        /** */
        NEAR_CACHE,

        /** */
        ENTRY_LOCK,

        /** */
        CACHE_EVENTS,

        /** */
        EVICTION,

        /** */
        EXPIRATION,

        /** */
        METRICS,

        /** */
        INTERCEPTOR
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
     * Fails if feature is not supported in Mvcc mode.
     *
     * @param feature Mvcc feature.
     * @throws AssertionError If failed.
     */
    private static String unsupportedReason(Feature feature) {
        switch (feature) {
            case NEAR_CACHE:
                return "https://issues.apache.org/jira/browse/IGNITE-7187";

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
