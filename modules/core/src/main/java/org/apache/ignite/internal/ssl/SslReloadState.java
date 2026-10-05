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

/**
 * Outcome of the reloads of one SSL context, by the {@code --ssl reload} command or by automatic renewal: when it last
 * succeeded, how it has been failing since, and when the next automatic renewal is due.
 */
public class SslReloadState {
    /** Time of the last successful reload, {@code 0} if there was none. */
    private volatile long lastSuccessTime;

    /** Time of the last failed reload since the last successful one, {@code 0} if there was none. */
    private volatile long lastFailureTime;

    /** Reason of the last failed reload since the last successful one, {@code null} if there was none. */
    private volatile String lastFailure;

    /** Failed reloads in a row since the last successful one. */
    private volatile int failures;

    /** Time of the next automatic renewal, {@code 0} if none is planned. */
    private volatile long nextRenewalTime;

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

    /** @return Time of the next automatic renewal, {@code 0} if none is planned. */
    public long nextRenewalTime() {
        return nextRenewalTime;
    }

    /**
     * @param nextRenewalTime Time of the next automatic renewal, {@code 0} if none is planned.
     */
    public void nextRenewalTime(long nextRenewalTime) {
        this.nextRenewalTime = nextRenewalTime;
    }

    /**
     * @param e Failure to describe.
     * @return Messages along its chain of causes, each once. A failure out of a user-supplied factory may carry no
     *      message at all, and is then named by its type.
     */
    public static String reason(Throwable e) {
        StringBuilder sb = new StringBuilder();

        int depth = 0;

        for (Throwable t = e; t != null && depth < 10; t = t.getCause(), depth++) {
            String msg = t.getMessage();

            // A wrapper made out of its cause alone carries nothing but the cause's own description.
            if (msg == null || msg.isEmpty() || (t.getCause() != null && msg.equals(t.getCause().toString())))
                continue;

            if (sb.indexOf(msg) >= 0)
                continue;

            if (sb.length() > 0)
                sb.append(": ");

            sb.append(msg);
        }

        return sb.length() > 0 ? sb.toString() : e.toString();
    }
}
