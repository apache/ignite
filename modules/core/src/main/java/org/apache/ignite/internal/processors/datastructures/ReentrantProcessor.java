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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/** */
public abstract class ReentrantProcessor<T>
    implements EntryProcessor<GridCacheInternalKey, GridCacheLockState2Base<T>, Boolean>, Externalizable {

    /** {@inheritDoc} */
    @Override public Boolean process(MutableEntry<GridCacheInternalKey, GridCacheLockState2Base<T>> entry,
        Object... objects) throws EntryProcessorException {

        assert entry != null;

        if (entry.exists()) {
            GridCacheLockState2Base<T> state = entry.getValue();

            LockedModified result = tryLock(state);

            // Write result if necessary
            if (result.modified) {
                entry.setValue(state);
            }

            return result.locked;
        }

        return false;
    }

    /** */
    protected abstract  LockedModified tryLock(GridCacheLockState2Base<T> state);
}
