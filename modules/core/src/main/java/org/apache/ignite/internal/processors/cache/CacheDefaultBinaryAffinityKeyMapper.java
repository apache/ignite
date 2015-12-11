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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class CacheDefaultBinaryAffinityKeyMapper extends GridCacheDefaultAffinityKeyMapper {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private CacheObjectBinaryProcessorImpl proc;

    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {
        try {
            key = proc.toBinary(key);
        }
        catch (IgniteException e) {
            U.error(log, "Failed to marshal key to binary: " + key, e);
        }

        if (key instanceof BinaryObject)
            return proc.affinityKey((BinaryObject)key);
        else
            return super.affinityKey(key);
    }

    /** {@inheritDoc} */
    @Override public void ignite(Ignite ignite) {
        super.ignite(ignite);

        if (ignite != null) {
            IgniteKernal kernal = (IgniteKernal)ignite;

            proc = (CacheObjectBinaryProcessorImpl)kernal.context().cacheObjects();
        }
    }
}
