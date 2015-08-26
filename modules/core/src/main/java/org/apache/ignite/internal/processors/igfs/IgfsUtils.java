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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Common IGFS utility methods.
 */
public class IgfsUtils {
    /** Maximum number of file unlock transaction retries when topology changes. */
    private static final int MAX_UNLOCK_TX_RETRIES = IgniteSystemProperties.getInteger(IGNITE_CACHE_RETRIES_COUNT, 100);

    /**
     * Converts any passed exception to IGFS exception.
     *
     * @param err Initial exception.
     * @return Converted IGFS exception.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public static IgfsException toIgfsException(Exception err) {
        IgfsException err0 = err instanceof IgfsException ? (IgfsException)err : null;

        IgfsException igfsErr = X.cause(err, IgfsException.class);

        while (igfsErr != null && igfsErr != err0) {
            err0 = igfsErr;

            igfsErr = X.cause(err, IgfsException.class);
        }

        // If initial exception is already IGFS exception and no inner stuff exists, just return it unchanged.
        if (err0 != err) {
            if (err0 != null)
                // Dealing with a kind of IGFS error, wrap it once again, preserving message and root cause.
                err0 = newIgfsException(err0.getClass(), err0.getMessage(), err0);
            else {
                if (err instanceof ClusterTopologyServerNotFoundException)
                    err0 = new IgfsException("Cache server nodes not found.", err);
                else
                    // Unknown error nature.
                    err0 = new IgfsException("Generic IGFS error occurred.", err);
            }
        }

        return err0;
    }

    /**
     * Construct new IGFS exception passing specified message and cause.
     *
     * @param cls Class.
     * @param msg Message.
     * @param cause Cause.
     * @return New IGFS exception.
     */
    public static IgfsException newIgfsException(Class<? extends IgfsException> cls, String msg, Throwable cause) {
        try {
            Constructor<? extends IgfsException> ctor = cls.getConstructor(String.class, Throwable.class);

            return ctor.newInstance(msg, cause);
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException("Failed to create IGFS exception: " + cls.getName(), e);
        }
    }

    /**
     * Constructor.
     */
    private IgfsUtils() {
        // No-op.
    }

    /**
     * Provides non-null user name.
     * If the user name is null or empty string, defaults to {@link FileSystemConfiguration#DFLT_USER_NAME},
     * which is the current process owner user.
     * @param user a user name to be fixed.
     * @return non-null interned user name.
     */
    public static String fixUserName(@Nullable String user) {
        if (F.isEmpty(user))
           user = FileSystemConfiguration.DFLT_USER_NAME;

        return user;
    }

    /**
     * Performs an operation with transaction with retries.
     *
     * @param cache Cache to do the transaction on.
     * @param clo Closure.
     * @return Result of closure execution.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public static <T> T doInTransactionWithRetries(IgniteInternalCache cache, IgniteOutClosureX<T> clo)
            throws IgniteCheckedException {
        assert cache != null;

        int attempts = 0;

        while (attempts < MAX_UNLOCK_TX_RETRIES) {
            attempts++;

            try (Transaction tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                T res = clo.applyx();

                tx.commit();

                return res;
            }
            catch (IgniteException | IgniteCheckedException e) {
                ClusterTopologyException cte = X.cause(e, ClusterTopologyException.class);

                if (cte != null)
                    cte.retryReadyFuture().get();
                else
                    throw U.cast(e);
            }
        }

        throw new IgniteCheckedException("Failed to perform operation since max number of attempts " +
            "exceeded. [maxAttempts=" + MAX_UNLOCK_TX_RETRIES + ']');
    }
}
