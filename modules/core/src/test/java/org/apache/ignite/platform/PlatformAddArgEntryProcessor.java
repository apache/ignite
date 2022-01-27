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

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.cache.CacheEntryProcessor;

/**
 * Entry processor that adds argument to cache entry.
 */
public class PlatformAddArgEntryProcessor implements CacheEntryProcessor<Object, Long, Long> {
    /** {@inheritDoc} */
    @Override public Long process(MutableEntry<Object, Long> mutableEntry, Object... args)
            throws EntryProcessorException {
        Long val = (Long)args[0];
        Long res = mutableEntry.getValue();
        res = res == null ? val : res + val;

        mutableEntry.setValue(res);

        return res;
    }
}
