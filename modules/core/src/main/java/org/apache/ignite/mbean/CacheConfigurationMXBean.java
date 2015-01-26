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

package org.apache.ignite.mbean;

import javax.cache.management.*;

/**
 *  A management bean for cache. It provides configuration information.
 */
@IgniteMBeanDescription("MBean that provides configuration information.")
public interface CacheConfigurationMXBean extends CacheMXBean {
    /** {@inheritDoc} */
    @IgniteMBeanDescription("Key type.")
    String getKeyType();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("Value type.")
    String getValueType();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("True if the cache is store by value.")
    boolean isStoreByValue();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("True if statistics collection is enabled.")
    boolean isStatisticsEnabled();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("True if management is enabled.")
    boolean isManagementEnabled();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("True when a cache is in read-through mode.")
    boolean isReadThrough();

    /** {@inheritDoc} */
    @IgniteMBeanDescription("True when a cache is in write-through mode.")
    boolean isWriteThrough();
}
