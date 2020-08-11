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

package org.apache.ignite.platform;

import java.io.Serializable;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Test filter factory
 */
public class PlatformCacheEntryEventFilterFactory implements Serializable,
    PlatformJavaObjectFactory<CacheEntryEventSerializableFilter> {
    /** Property to be set from platform. */
    private String startsWith = "-";

    /** Injected instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public CacheEntryEventSerializableFilter create() {
        assert ignite != null;

        return new CacheEntryEventSerializableFilter() {
            @Override public boolean evaluate(CacheEntryEvent event) throws CacheEntryListenerException {
                Object value = event.getValue();

                if (value instanceof String)
                    return ((String)value).startsWith(startsWith);

                assert value instanceof BinaryObject;

                return ((String)((BinaryObject)value).field("String")).startsWith(startsWith);
            }
        };
    }
}
