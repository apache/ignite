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

import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.stream.StreamReceiver;

/**
 * IgniteSandbox executes a user-defined code with restrictions.
 */
public interface IgniteSandbox {
    /**
     * Executes {@code callable} with constraints defined by current {@code SecuritySubject}.
     *
     * @param call Callable to execute.
     * @return Result of {@code callable}.
     * @see IgniteSecurity#withContext(UUID)
     * @see IgniteSecurity#withContext(SecurityContext)
     * @see SecuritySubject#sandboxPermissions()
     */
    public <T> T execute(Callable<T> call) throws IgniteException;

    /**
     * Executes {@code runnable} with constraints defined by current {@code SecuritySubject}.
     *
     * @param runnable Runnable to execute.
     * @see IgniteSecurity#withContext(UUID)
     * @see IgniteSecurity#withContext(SecurityContext)
     * @see SecuritySubject#sandboxPermissions()
     */
    public default void execute(Runnable runnable) throws IgniteException {
        execute(() -> {
            runnable.run();

            return null;
        });
    }

    /**
     * Wraps ComputeJob to execute with restriction.
     *
     * @param job ComputeJob to wrap.
     * @return Wrapped ComputeJob.
     */
    public ComputeJob wrapper(ComputeJob job);

    /**
     * Wraps EntryProcessor to execute with restriction.
     *
     * @param prc EntryProcessor to wrap.
     * @return Wrapped EntryProcessor.
     */
    public <K, V, T> EntryProcessor<K, V, T> wrapper(EntryProcessor<K, V, T> prc);

    /**
     * Wraps IgniteBiPredicate to execute with restriction.
     *
     * @param p IgniteBiPredicate to wrap.
     * @return Wrapped IgniteBiPredicate.
     */
    public <K, V> IgniteBiPredicate<K, V> wrapper(IgniteBiPredicate<K, V> p);

    /**
     * Wraps IgniteClosure to execute with restriction.
     *
     * @param c IgniteClosure to wrap.
     * @return Wrapped IgniteClosure.
     */
    public <E, R> IgniteClosure<E, R> wrapper(IgniteClosure<E, R> c);

    /**
     * Wraps StreamReceiver to execute with restriction.
     *
     * @param r StreamReceiver to wrap.
     * @return Wrapped StreamReceiver.
     */
    public <K, V> StreamReceiver<K, V> wrapper(StreamReceiver<K, V> r);
}
