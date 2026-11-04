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

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslBundle;
import org.jetbrains.annotations.Nullable;

/** Operations the node uses to report and replace a factory's TLS material. */
public abstract class SslFactoryAccess {
    /** @return Material in use, initialized on first use. */
    protected abstract SslBundle current(AbstractSslContextFactory factory);

    /** @return Fresh material accepted by the factory. */
    protected abstract SslBundle reload(AbstractSslContextFactory factory);

    /** @return Last successful reload and any failures since then. */
    protected abstract ReloadOutcome reloadOutcome(AbstractSslContextFactory factory);

    /** Attaches this node's renewal executor. */
    protected abstract void startRenewal(
        AbstractSslContextFactory factory,
        ScheduledExecutorService exec,
        IgniteLogger log,
        Supplier<String> transports
    );

    /** Renews the material if the scheduled context is still current. */
    protected abstract @Nullable SslBundle renew(AbstractSslContextFactory factory, SSLContext expected) throws IgniteCheckedException;

    /** Reads the unsuccessful attempt count. */
    protected abstract int renewalFailures(AbstractSslContextFactory factory);

    /** Reads the time of the next planned attempt. */
    protected abstract long nextRenewalTime(AbstractSslContextFactory factory);

    /**
     * Snapshot of the last successful reload and subsequent failures.
     *
     * @param successTime Time of the last successful reload, {@code 0} if there was none.
     * @param failureTime Time of the last failed reload since the last successful one, {@code 0} if there was none.
     * @param failureReason Reason of the last failed reload since the last successful one, {@code null} if there was none.
     * @param failures Failed reloads in a row since the last successful one, {@code 0} if there were none.
     */
    public record ReloadOutcome(long successTime, long failureTime, @Nullable String failureReason, int failures) {
        // No-op.
    }
}
