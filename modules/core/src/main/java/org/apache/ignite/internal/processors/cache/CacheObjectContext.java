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
import org.apache.ignite.internal.processors.cacheobject.*;

import java.util.*;

/**
 *
 */
public class CacheObjectContext {
    /** */
    private GridKernalContext kernalCtx;

    /** */
    private IgniteCacheObjectProcessor proc;

    /** */
    private AffinityKeyMapper dfltAffMapper;

    /** */
    private boolean cpyOnGet;

    /** */
    private boolean unmarshalVals;

    /** */
    private boolean p2pEnabled;

    /**
     * @param kernalCtx Kernal context.
     * @param dfltAffMapper Default affinity mapper.
     * @param cpyOnGet Copy on get flag.
     * @param unmarshalVals Unmarshal values flag.
     */
    public CacheObjectContext(GridKernalContext kernalCtx,
        AffinityKeyMapper dfltAffMapper,
        boolean cpyOnGet,
        boolean unmarshalVals) {
        this.kernalCtx = kernalCtx;
        this.p2pEnabled = kernalCtx.config().isPeerClassLoadingEnabled();
        this.dfltAffMapper = dfltAffMapper;
        this.cpyOnGet = cpyOnGet;
        this.unmarshalVals = unmarshalVals;

        proc = kernalCtx.cacheObjects();
    }

    /**
     * @return {@code True} if peer class loading is enabled.
     */
    public boolean p2pEnabled() {
        return p2pEnabled;
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
    public AffinityKeyMapper defaultAffMapper() {
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
    public IgniteCacheObjectProcessor processor() {
        return proc;
    }

    /**
     * Unwraps object.
     *
     * @param o Object to unwrap.
     * @param keepPortable Keep portable flag.
     * @return Unwrapped object.
     */
    public Object unwrapPortableIfNeeded(Object o, boolean keepPortable) {
        return o;
    }

    /**
     * Unwraps collection.
     *
     * @param col Collection to unwrap.
     * @param keepPortable Keep portable flag.
     * @return Unwrapped collection.
     */
    public Collection<Object> unwrapPortablesIfNeeded(Collection<Object> col, boolean keepPortable) {
        return col;
    }
}
