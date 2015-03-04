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

import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.portable.*;

/**
 *
 */
public class CacheObjectContext {
    /** */
    private GridKernalContext kernalCtx;

    /** */
    private GridPortableProcessor proc;

    /** */
    private CacheAffinityKeyMapper dfltAffMapper;

    /** */
    private boolean cpyOnGet;

    /** */
    private boolean unmarshalVals;

    /**
     * @param kernalCtx Kernal context.
     * @param dfltAffMapper Default affinity mapper.
     * @param cpyOnGet Copy on get flag.
     * @param unmarshalVals Unmarshal values flag.
     */
    public CacheObjectContext(GridKernalContext kernalCtx,
        CacheAffinityKeyMapper dfltAffMapper,
        boolean cpyOnGet,
        boolean unmarshalVals) {
        this.kernalCtx = kernalCtx;
        this.dfltAffMapper = dfltAffMapper;
        this.cpyOnGet = cpyOnGet;
        this.unmarshalVals = unmarshalVals;

        proc = kernalCtx.portable();
    }

    /**
     * @return Copy on get flag.
     */
    public boolean copyOnGet() {
        return cpyOnGet;
    }

    /**
     * @return Unmarshal values flag.
     */
    public boolean unmarshalValues() {
        return unmarshalVals;
    }

    /**
     * @return Default affinity mapper.
     */
    public CacheAffinityKeyMapper defaultAffMapper() {
        return dfltAffMapper;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return kernalCtx;
    }

    /**
     * @return Processor.
     */
    public GridPortableProcessor processor() {
        return proc;
    }
}
