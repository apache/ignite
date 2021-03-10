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

import java.util.concurrent.Callable;

/**
 * The callable executes the call method with a security context that was actual when the calleble was created.
 */
public class SecurityAwareCallable<T> extends SecurityAwareAdapter implements Callable<T> {
    /** Original callable. */
    private final Callable<T> original;

    /** */
    public SecurityAwareCallable(IgniteSecurity security, Callable<T> original) {
        super(security);

        this.original = original;
    }

    /** {@inheritDoc} */
    @Override public T call() throws Exception {
        try (OperationSecurityContext c = withContext()) {
            return original.call();
        }
    }
}
