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

package org.apache.ignite.internal.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;

/**
 * Class for avoid multiple initialization of specific value from various threads.
 */
public class InitializationProtector {
    /** Holder of locks */
    ConcurrentHashMap<Object, ReentrantLock> protectedObjects = new ConcurrentHashMap<>();

    /**
     * @param protectedKey Unique value by which initialization code should be run only one time.
     * @param initializedVal Supplier for given already initialized value if it exist or null as sign that
     * initialization required.
     * @param initializationCode Code for initialization value corresponding protectedKey.
     * @param <T> Type of initialization value.
     * @return Initialized value.
     * @throws IgniteCheckedException if initialization was failed.
     */
    public <T> T protect(Object protectedKey, Supplier<T> initializedVal,
        IgniteThrowableRunner initializationCode) throws IgniteCheckedException {
        T value = initializedVal.get();

        if (value != null)
            return value;

        ReentrantLock lock = protectedObjects.computeIfAbsent(protectedKey, (k) -> new ReentrantLock());

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

            protectedObjects.remove(protectedKey, lock);
        }
    }
}
