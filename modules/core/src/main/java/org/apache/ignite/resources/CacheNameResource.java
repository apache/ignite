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

package org.apache.ignite.resources;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a field or a setter method for injection of grid cache name.
 * Grid cache name is provided to cache via {@link org.apache.ignite.configuration.CacheConfiguration#getName()} method.
 * <p>
 * Cache name can be injected into components provided in the {@link org.apache.ignite.configuration.CacheConfiguration},
 * if {@link CacheNameResource} annotation is used in another classes it is no-op.
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyCacheStore implements GridCacheStore {
 *      ...
 *      &#64;CacheNameResource
 *      private String cacheName;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyCacheStore implements GridCacheStore {
 *     ...
 *     private String cacheName;
 *     ...
 *     &#64;CacheNameResource
 *     public void setCacheName(String cacheName) {
 *          this.cacheName = cacheName;
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 * See {@link org.apache.ignite.configuration.CacheConfiguration#getName()} for cache configuration details.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface CacheNameResource {
    // No-op.
}