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

package org.gridgain.grid.cache;

import org.gridgain.grid.cache.affinity.*;

import java.lang.annotation.*;
import java.util.concurrent.*;

/**
 * Allows to specify cache name from grid computations. It is used to provide cache name
 * for affinity routing of grid computations, such as {@link org.apache.ignite.compute.ComputeJob}, {@link Runnable},
 * {@link Callable}, or {@link org.apache.ignite.lang.IgniteClosure}. It should be used only in conjunction with
 * {@link GridCacheAffinityKeyMapped @GridCacheAffinityKeyMapped} annotation, and should be attached to a method or field
 * that provides cache name for the computation. Only one annotation per class
 * is allowed. In the absence of this annotation, the default no-name cache
 * will be used for providing key-to-node affinity.
 * <p>
 * Refer to {@link GridCacheAffinityKeyMapped @GridCacheAffinityKeyMapped} documentation for more information
 * and examples about this annotation.
 * @see GridCacheAffinityKeyMapped
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface GridCacheName {
    // No-op.
}
