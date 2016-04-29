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

package org.apache.ignite.testframework.configvariations;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Configuration factory.
 */
public interface ConfigFactory {
    /**
     * @param gridName Grid name.
     * @param srcCfg Source config.
     * @return IgniteConfiguration.
     */
    public IgniteConfiguration getConfiguration(String gridName, IgniteConfiguration srcCfg);

    /**
     * @param gridName Name.
     * @return CacheConfiguration.
     */
    public CacheConfiguration cacheConfiguration(String gridName);
}
