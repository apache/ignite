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

import java.util.concurrent.Callable;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.stream.StreamReceiver;

/**
 * No operation Sandbox.
 */
public class NoOpSandbox implements IgniteSandbox {
    /** {@inheritDoc} */
    @Override public <T> T execute(Callable<T> call) throws IgniteException {
        try {
            return call.call();
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeJob wrapper(ComputeJob job) {
        return job;
    }

    /** {@inheritDoc} */
    @Override public <K, V, T> EntryProcessor<K, V, T> wrapper(EntryProcessor<K, V, T> prc) {
        return prc;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteBiPredicate<K, V> wrapper(IgniteBiPredicate<K, V> p) {
        return p;
    }

    /** {@inheritDoc} */
    @Override public <E, R> IgniteClosure<E, R> wrapper(IgniteClosure<E, R> c) {
        return c;
    }

    /** {@inheritDoc} */
    @Override public <K, V> StreamReceiver<K, V> wrapper(StreamReceiver<K, V> r) {
        return r;
    }
}
