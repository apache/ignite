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

package org.apache.ignite.internal.processors.security.sandbox.closure;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.security.sandbox.IgniteSandbox;
import org.apache.ignite.stream.StreamReceiver;

/**
 * Wrapper for {@link StreamReceiver} that executes its {@code receive} method with restriction defined by current
 * security context.
 *
 * @see IgniteSandbox#execute(java.util.concurrent.Callable)
 */
public class SandboxAwareStreamReceiver<K, V> implements StreamReceiver<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteSandbox sandbox;

    /** */
    private final StreamReceiver<K, V> original;

    /** */
    public SandboxAwareStreamReceiver(IgniteSandbox sandbox, StreamReceiver<K, V> original) {
        this.sandbox = Objects.requireNonNull(sandbox, "Sandbox cannot be null.");
        this.original = Objects.requireNonNull(original, "Original cannot be null.");
    }

    /** {@inheritDoc} */
    @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) throws IgniteException {
        sandbox.execute(() -> original.receive(cache, entries));
    }
}
