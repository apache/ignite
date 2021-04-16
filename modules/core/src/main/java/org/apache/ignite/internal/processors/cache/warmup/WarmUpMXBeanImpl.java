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

package org.apache.ignite.internal.processors.cache.warmup;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.WarmUpMXBean;

/**
 * {@link WarmUpMXBean} implementation.
 */
public class WarmUpMXBeanImpl implements WarmUpMXBean {
    /** Cache processor. */
    @GridToStringExclude
    private final GridCacheProcessor cacheProc;

    /**
     * Constructor.
     *
     * @param cacheProc Cache processor.
     */
    public WarmUpMXBeanImpl(GridCacheProcessor cacheProc) {
        this.cacheProc = cacheProc;
    }

    /** {@inheritDoc} */
    @Override public void stopWarmUp() {
        try {
            cacheProc.stopWarmUp();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WarmUpMXBeanImpl.class, this);
    }
}
