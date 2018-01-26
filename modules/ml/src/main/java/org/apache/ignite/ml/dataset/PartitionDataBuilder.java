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

package org.apache.ignite.ml.dataset;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 *
 * @param <K>
 * @param <V>
 * @param <C>
 * @param <D>
 */
@FunctionalInterface
public interface PartitionDataBuilder<K, V, C extends Serializable, D extends AutoCloseable> extends Serializable {
    /**
     *
     * @param upstreamData
     * @param upstreamDataSize
     * @param ctx
     * @return
     */
    public D build(Iterator<PartitionUpstreamEntry<K, V>> upstreamData, long upstreamDataSize, C ctx);

    /**
     *
     * @param fun
     * @param <D2>
     * @return
     */
    default public <D2 extends AutoCloseable> PartitionDataBuilder<K, V, C, D2> andThen(IgniteBiFunction<D, C, D2> fun) {
       return (upstreamData, upstreamDataSize, ctx) -> fun.apply(build(upstreamData, upstreamDataSize, ctx), ctx);
    }
}
