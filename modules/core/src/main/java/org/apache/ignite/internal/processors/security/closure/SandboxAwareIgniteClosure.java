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
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.lang.IgniteClosure;

/**
 * Wrapper for {@link IgniteClosure} that executes its {@code apply} method with restriction defined by current security
 * context.
 *
 * @see IgniteSecurity#execute(java.util.concurrent.Callable)
 */
public class SandboxAwareIgniteClosure<E, R> implements IgniteClosure<E, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteSecurity security;

    /** */
    private final IgniteClosure<E, R> original;

    /** */
    public SandboxAwareIgniteClosure(IgniteSecurity security, IgniteClosure<E, R> original) {
        this.security = Objects.requireNonNull(security, "Security cannot be null.");
        this.original = Objects.requireNonNull(original, "Original cannot be null.");
    }

    /** {@inheritDoc} */
    @Override public R apply(E e) {
        return security.execute(() -> original.apply(e));
    }
}
