/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.errors;

import javax.cache.CacheException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;

/**
 * Class with error codes and messages.
 */
public class Errors {
    /** */
    public static final int ERR_EMAIL_NOT_CONFIRMED = 10104;

    /**
     * @param e Exception to check.
     * @return {@code true} if database not available.
     */
    public static boolean checkDatabaseNotAvailable(Throwable e) {
        if (e instanceof IgniteClientDisconnectedException)
            return true;

        if (e instanceof CacheException && e.getCause() instanceof IgniteClientDisconnectedException)
            return true;

        // TODO GG-19681: In future versions specific exception will be added.
        String msg = e.getMessage();

        return e instanceof IgniteException &&
            msg != null &&
            msg.startsWith("Cannot start/stop cache within lock or transaction");
    }


    /**
     * @param e Exception.
     * @param msg Message.
     * @return Exception to throw.
     */
    public static RuntimeException convertToDatabaseNotAvailableException(RuntimeException e, String msg) {
        return checkDatabaseNotAvailable(e) ? new DatabaseNotAvailableException(msg) : e;
    }
}
