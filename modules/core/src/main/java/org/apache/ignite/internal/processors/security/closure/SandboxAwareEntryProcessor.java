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

import java.util.Objects;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.internal.processors.security.IgniteSecurity;

/**
 * Wrapper for {@link EntryProcessor} that executes its {@code process} method with restriction defined by current
 * security context.
 *
 * @see IgniteSecurity#execute(java.util.concurrent.Callable)
 */
public class SandboxAwareEntryProcessor<K, V, T> implements EntryProcessor<K, V, T> {
    /** */
    private final IgniteSecurity sec;

    /** */
    private final EntryProcessor<K, V, T> origin;

    /** */
    public SandboxAwareEntryProcessor(IgniteSecurity sec, EntryProcessor<K, V, T> origin) {
        this.sec = Objects.requireNonNull(sec, "Sec cannot be null.");
        this.origin = Objects.requireNonNull(origin, "Orgin cannot be null.");
    }

    /** {@inheritDoc} */
    @Override public T process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
        return sec.execute(() -> origin.process(entry, arguments));
    }
}
