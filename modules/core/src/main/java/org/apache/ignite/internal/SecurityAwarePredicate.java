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

package org.apache.ignite.internal;

import java.security.AccessControlException;
import java.util.UUID;
import org.apache.ignite.internal.processors.security.AbstractSecurityAwareExternalizable;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.security.sandbox.IgniteSandbox;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Security aware IgnitePredicate.
 */
public class SecurityAwarePredicate<E> extends AbstractSecurityAwareExternalizable<IgnitePredicate<E>>
    implements IgnitePredicate<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public SecurityAwarePredicate() {
        // No-op.
    }

    /**
     * @param subjectId Security subject id.
     * @param original Original predicate.
     */
    public SecurityAwarePredicate(UUID subjectId, IgnitePredicate<E> original) {
        super(subjectId, original);
    }

    /** {@inheritDoc} */
    @Override public boolean apply(E evt) {
        IgniteSecurity security = ignite.context().security();

        try (OperationSecurityContext c = security.withContext(subjectId)) {
            IgniteSandbox sandbox = security.sandbox();

            return sandbox.enabled() ? sandbox.execute(() -> original.apply(evt)) : original.apply(evt);
        }
        catch (AccessControlException e) {
            logAccessDeniedMessage(e);

            throw e;
        }
    }
}
