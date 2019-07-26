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

package org.apache.ignite.internal.processors.security.closure;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.stream.StreamReceiver;

/**
 * Wrapper for {@link StreamReceiver} that executes its {@code receive} method with restriction defined by current
 * security context.
 *
 * @see IgniteSecurity#execute(java.util.concurrent.Callable)
 */
public class SandboxAwareStreamReceiver<K, V> implements StreamReceiver<K, V> {
    /** */
    private static final long serialVersionUID = 3783092194186094866L;

    /** */
    private final IgniteSecurity sec;

    /** */
    private final StreamReceiver<K, V> origin;

    /** */
    public SandboxAwareStreamReceiver(IgniteSecurity sec, StreamReceiver<K, V> origin) {
        this.sec = Objects.requireNonNull(sec, "Sec cannot be null.");
        this.origin = Objects.requireNonNull(origin, "Origin cannot be null.");
    }

    /** {@inheritDoc} */
    @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> entries) throws IgniteException {
        sec.execute(() -> origin.receive(cache, entries));
    }
}
