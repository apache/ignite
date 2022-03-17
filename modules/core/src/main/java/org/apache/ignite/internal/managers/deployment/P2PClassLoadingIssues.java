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

package org.apache.ignite.internal.managers.deployment;

import org.apache.ignite.IgniteException;

/**
 *
 */
public class P2PClassLoadingIssues {
    /**
     * If the given error is related to p2p class-loading, it's converted to {@link IgniteException} and rethrown;
     * otherwise, the original error is rethrown.
     *
     * @param error error to check
     * @param <T>   declared return type
     * @return this method never returns normally, it always throws something
     */
    public static <T> T rethrowDisarmedP2PClassLoadingFailure(NoClassDefFoundError error) {
        if (isP2PClassLoadingFailure(error)) {
            throw new IgniteException("P2P class loading failed", error);
        }
        throw error;
    }

    /**
     * Returns @{code true} if the given Throwable is an error caused by a P2P class-loading failure.
     *
     * @param error Throwable to check
     * @return @{code true} if the given Throwable is an error caused by a P2P class-loading failure
     */
    private static boolean isP2PClassLoadingFailure(NoClassDefFoundError error) {
        return error instanceof NoClassDefFoundError && error.getCause() != null
            && error.getCause() instanceof P2PClassNotFoundException;
    }

    /***/
    private P2PClassLoadingIssues() {
        // prevent instantiation
    }
}
