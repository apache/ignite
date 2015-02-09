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

package org.apache.ignite.cache.cloner;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

/**
 * Cache cloner which clones cached values before returning them from cache.
 * It will only be used if {@link org.apache.ignite.internal.processors.cache.CacheFlag#CLONE} flag is set
 * on projection which the user is working with (this flag is disabled
 * by default).
 * <p>
 * This behaviour is useful, as a an example, when we need to get some object
 * from cache, change some of its properties and put it back into cache.
 * In such a scenario it would be wrong to change properties of cached value
 * itself without creating a copy first, since it would break cache integrity,
 * and will affect the cached values returned to other threads even before
 * the transaction commits.
 * <p>
 * Cache cloner can be set in cache configuration via {@link org.apache.ignite.configuration.CacheConfiguration#getCloner()}
 * method. By default, cache uses {@link CacheBasicCloner} implementation
 * which will clone only objects implementing {@link Cloneable} interface. You
 * can also configure cache to use {@link CacheDeepCloner} which will perform
 * deep-cloning of all objects returned from cache, regardless of the
 * {@link Cloneable} interface. If none of the above cloners fit your logic, you
 * can also provide your own implementation of this interface.
 *
 * @see CacheBasicCloner
 * @see CacheDeepCloner
 * @see org.apache.ignite.configuration.CacheConfiguration#getCloner()
 * @see org.apache.ignite.configuration.CacheConfiguration#setCloner(CacheCloner)
 *
 */
public interface CacheCloner {
    /**
     * @param val Object to make a clone for.
     * @throws IgniteCheckedException If failed to clone given object.
     * @return Clone for given object.
     */
    @Nullable public <T> T cloneValue(T val) throws IgniteCheckedException;
}
