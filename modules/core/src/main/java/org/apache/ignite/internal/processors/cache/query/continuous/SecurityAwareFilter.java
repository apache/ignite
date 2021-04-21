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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.security.AccessControlException;
import java.util.UUID;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.internal.processors.security.AbstractSecurityAwareExternalizable;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.security.sandbox.IgniteSandbox;

/**
 * Security aware remote filter.
 */
public class SecurityAwareFilter<K, V> extends AbstractSecurityAwareExternalizable<CacheEntryEventFilter<K, V>>
    implements CacheEntryEventSerializableFilter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public SecurityAwareFilter() {
        // No-op.
    }

    /**
     * @param subjectId Security subject id.
     * @param original Original filter.
     */
    public SecurityAwareFilter(UUID subjectId, CacheEntryEventFilter<K, V> original) {
        super(subjectId, original);
    }

    /** {@inheritDoc} */
    @Override public boolean evaluate(CacheEntryEvent<? extends K, ? extends V> evt) throws CacheEntryListenerException {
        IgniteSecurity security = ignite.context().security();

        try (OperationSecurityContext c = security.withContext(subjectId)) {
            IgniteSandbox sandbox = security.sandbox();

            return sandbox.enabled() ? sandbox.execute(() -> original.evaluate(evt)) : original.evaluate(evt);
        }
        catch (AccessControlException e) {
            logAccessDeniedMessage(e);

            throw e;
        }
    }
}
