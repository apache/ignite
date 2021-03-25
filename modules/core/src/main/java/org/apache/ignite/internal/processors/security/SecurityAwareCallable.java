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

package org.apache.ignite.internal.processors.security;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridKernalContext;

/**
 * The callable executes the call method with a security context that was actual when the calleble was created.
 */
public class SecurityAwareCallable<T> extends SecurityAwareAdapter implements Callable<T> {
    /** */
    public static <A> Collection<? extends Callable<A>> convertToSecurityAware(GridKernalContext ctx,
        Collection<? extends Callable<A>> tasks) {
        return tasks.stream().map(t -> new SecurityAwareCallable<>(ctx, t)).collect(Collectors.toList());
    }

    /** Original callable. */
    private final Callable<T> original;

    /** */
    public SecurityAwareCallable(GridKernalContext ctx, Callable<T> original) {
        super(ctx);

        this.original = original;
    }

    /** {@inheritDoc} */
    @Override public T call() throws Exception {
        try (OperationSecurityContext c = withContext()) {
            return original.call();
        }
    }
}
