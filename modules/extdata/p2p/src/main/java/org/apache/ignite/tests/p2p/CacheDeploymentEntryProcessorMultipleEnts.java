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

import java.util.Map;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.tests.p2p.cache.Container;

/**
 * Entry processor for p2p tests.
 */
public class CacheDeploymentEntryProcessorMultipleEnts implements CacheEntryProcessor<String, CacheDeploymentTestValue, Boolean> {
    /** */
    private Map<Long, CacheDeploymentTestValue> entToProcess;

    /** */
    public CacheDeploymentEntryProcessorMultipleEnts(Object container) {
        entToProcess = (Map<Long, CacheDeploymentTestValue>)((Container)container).field;
    }

    /** {@inheritDoc} */
    @Override public Boolean process(MutableEntry<String, CacheDeploymentTestValue> entry,
            Object... arguments) throws EntryProcessorException {
        boolean pr = false;

        for (CacheDeploymentTestValue ent : entToProcess.values()) {
            CacheDeploymentTestValue key = ent;
            pr = key != null;
        }
        CacheDeploymentTestValue val = entry.getValue();

        return pr;
    }
}
