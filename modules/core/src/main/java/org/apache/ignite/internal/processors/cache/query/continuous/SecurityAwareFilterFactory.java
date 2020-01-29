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

import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.internal.processors.security.AbstractSecurityAwareExternalizable;

/**
 * Security aware remote filter factory.
 */
public class SecurityAwareFilterFactory<K, V> extends
    AbstractSecurityAwareExternalizable<Factory<CacheEntryEventFilter<K, V>>> implements
    Factory<CacheEntryEventFilter<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public SecurityAwareFilterFactory() {
        // No-op.
    }

    /**
     * @param subjectId Security subject id.
     * @param original Original factory.
     */
    public SecurityAwareFilterFactory(UUID subjectId, Factory<CacheEntryEventFilter<K, V>> original) {
        super(subjectId, original);
    }

    /** {@inheritDoc} */
    @Override public CacheEntryEventFilter<K, V> create() {
        return new SecurityAwareFilter<>(subjectId, original.create());
    }
}
