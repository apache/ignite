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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class FragmentDescription implements Serializable {
    /**
     *
     */
    private long fragmentId;

    /**
     *
     */
    private FragmentMapping mapping;

    /**
     *
     */
    private ColocationGroup target;

    /**
     *
     */
    private Long2ObjectOpenHashMap<List<String>> remoteSources;

    /**
     *
     */
    public FragmentDescription() {
    }

    /**
     *
     */
    public FragmentDescription(long fragmentId, FragmentMapping mapping, ColocationGroup target,
            Long2ObjectOpenHashMap<List<String>> remoteSources) {
        this.fragmentId = fragmentId;
        this.mapping = mapping;
        this.target = target;
        this.remoteSources = remoteSources;
    }

    /**
     *
     */
    public long fragmentId() {
        return fragmentId;
    }

    /**
     *
     */
    public List<String> nodeIds() {
        return mapping.nodeIds();
    }

    /**
     *
     */
    public ColocationGroup target() {
        return target;
    }

    /**
     *
     */
    public Long2ObjectOpenHashMap<List<String>> remotes() {
        return remoteSources;
    }

    /**
     *
     */
    public FragmentMapping mapping() {
        return mapping;
    }
}
