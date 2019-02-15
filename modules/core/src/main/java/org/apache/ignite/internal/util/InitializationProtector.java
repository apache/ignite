/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util;

import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;

/**
 * Class for avoid multiple initialization of specific value from various threads.
 */
public class InitializationProtector {
    /** Default striped lock concurrency level. */
    private static final int DEFAULT_CONCURRENCY_LEVEL = Runtime.getRuntime().availableProcessors();

    /** Striped lock. */
    private GridStripedLock stripedLock = new GridStripedLock(DEFAULT_CONCURRENCY_LEVEL);

    /**
     * @param protectedKey Unique value by which initialization code should be run only one time.
     * @param initializedVal Supplier for given already initialized value if it exist or null as sign that
     * initialization required.
     * @param initializationCode Code for initialization value corresponding protectedKey. Should be idempotent.
     * @param <T> Type of initialization value.
     * @return Initialized value.
     * @throws IgniteCheckedException if initialization was failed.
     */
    public <T> T protect(Object protectedKey, Supplier<T> initializedVal,
        IgniteThrowableRunner initializationCode) throws IgniteCheckedException {
        T value = initializedVal.get();

        if (value != null)
            return value;

        Lock lock = stripedLock.getLock(protectedKey.hashCode() % stripedLock.concurrencyLevel());

        lock.lock();
        try {
            value = initializedVal.get();

            if (value != null)
                return value;

            initializationCode.run();

            return initializedVal.get();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * It method allows to avoid simultaneous initialization from various threads.
     *
     * @param protectedKey Unique value by which initialization code should be run only from one thread in one time.
     * @param initializationCode Code for initialization value corresponding protectedKey. Should be idempotent.
     * @throws IgniteCheckedException if initialization was failed.
     */
    public void protect(Object protectedKey, IgniteThrowableRunner initializationCode) throws IgniteCheckedException {
        protect(protectedKey, () -> null, initializationCode);
    }
}
