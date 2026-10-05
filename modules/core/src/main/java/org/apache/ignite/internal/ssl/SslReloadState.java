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

package org.apache.ignite.internal.ssl;

import org.jetbrains.annotations.Nullable;

/** Outcome of the reloads of one SSL context: when it last succeeded, and how it has been failing since. */
public class SslReloadState {
    /** Time of the last successful reload, {@code 0} if there was none. */
    private volatile long lastSuccessTime;

    /** Time of the last failed reload since the last successful one, {@code 0} if there was none. */
    private volatile long lastFailureTime;

    /** Reason of the last failed reload since the last successful one, {@code null} if there was none. */
    private volatile String lastFailure;

    /** Failed reloads in a row since the last successful one. */
    private volatile int failures;

    /** Records a successful reload. */
    public synchronized void onSuccess() {
        lastSuccessTime = System.currentTimeMillis();
        lastFailureTime = 0;
        lastFailure = null;
        failures = 0;
    }

    /**
     * Records a failed reload.
     *
     * @param reason What went wrong.
     */
    public synchronized void onFailure(String reason) {
        lastFailureTime = System.currentTimeMillis();
        lastFailure = reason;
        failures++;
    }

    /** @return Time of the last successful reload, {@code 0} if there was none. */
    public long lastSuccessTime() {
        return lastSuccessTime;
    }

    /** @return Time of the last failed reload since the last successful one, {@code 0} if there was none. */
    public long lastFailureTime() {
        return lastFailureTime;
    }

    /** @return Reason of the last failed reload since the last successful one, {@code null} if there was none. */
    public @Nullable String lastFailure() {
        return lastFailure;
    }

    /** @return Failed reloads in a row since the last successful one. */
    public int failures() {
        return failures;
    }
}
