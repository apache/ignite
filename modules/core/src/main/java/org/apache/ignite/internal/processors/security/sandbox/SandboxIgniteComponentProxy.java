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

package org.apache.ignite.internal.processors.security.sandbox;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.internal.processors.security.SecurityUtils;

/** Create instace of Ignite component proxy to use inside the Ignite Sandbox. */
public final class SandboxIgniteComponentProxy {
    /** The array of classes that should be proxied. */
    private static final Class[] PROXIED_CLASSES = new Class[] {
        Ignite.class,
        IgniteCache.class,
        IgniteCompute.class,
        ExecutorService.class,
        IgniteScheduler.class,
        IgniteTransactions.class,
        IgniteDataStreamer.class,
        IgniteAtomicSequence.class,
        IgniteAtomicLong.class,
        IgniteAtomicReference.class,
        IgniteAtomicStamped.class,
        IgniteCountDownLatch.class,
        IgniteSemaphore.class,
        IgniteLock.class,
        IgniteQueue.class,
        IgniteSet.class,
        IgniteBinary.class,
        Affinity.class
    };

    /**
     * @return The proxy of {@code instance} to use inside the Ignite Sandbox.
     */
    public static <T> T proxy(Class cls, T instance) {
        Objects.requireNonNull(cls, "Parameter 'cls' cannot be null.");
        Objects.requireNonNull(instance, "Parameter 'instance' cannot be null.");

        return SecurityUtils.doPrivileged(
            () -> (T)Proxy.newProxyInstance(cls.getClassLoader(), new Class[] {cls},
                new SandboxIgniteComponentProxyHandler(instance))
        );
    }

    /** */
    private static class SandboxIgniteComponentProxyHandler implements InvocationHandler {
        /** */
        private final Object original;

        /** */
        public SandboxIgniteComponentProxyHandler(Object original) {
            this.original = original;
        }

        /** {@inheritDoc} */
        @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
            Object res = SecurityUtils.doPrivileged(() -> mtd.invoke(original, args));

            Class cls = proxiedClass(res);

            return cls != null ? proxy(cls, res) : res;
        }

        /** */
        private Class proxiedClass(Object obj) {
            for (Class cls : PROXIED_CLASSES) {
                if (cls.isInstance(obj))
                    return cls;
            }

            return null;
        }
    }
}
