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

package org.apache.ignite.tests.p2p;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 *
 */
public class CacheDeploymenComputeWithNestedEntryProcessor implements IgniteCallable<Boolean> {
    /** */
    @IgniteInstanceResource
    Ignite ignite;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Key. */
    private int key;

    /** Cache name. */
    private String cacheName;

    /**
     * Default constructor.
     */
    public CacheDeploymenComputeWithNestedEntryProcessor() {
        // No-op.
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     */
    public CacheDeploymenComputeWithNestedEntryProcessor(String cacheName, int key) {
        this.key = key;
        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override public Boolean call() {
        log.info("!!!!! I am Compute with nested entry processor " + key + " on " + ignite.name());

        return ignite.cache(cacheName).withKeepBinary().invoke(key, new NestedEntryProcessor());
    }

    /** */
    private static class NestedEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            return entry.getValue() != null;
        }
    }
}
