/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgniteCacheReadThroughEvictionsVariationsSuite {
    /** */
    public static List<Class<?>> suite() {
        return new ConfigVariationsTestSuiteBuilder(IgniteCacheReadThroughEvictionSelfTest.class)
            .withBasicCacheParams()
            .withIgniteConfigFilters(new IgnitePredicate<IgniteConfiguration>() {
                /** {@inheritDoc} */
                @SuppressWarnings("RedundantIfStatement")
                @Override public boolean apply(IgniteConfiguration cfg) {
                    if (cfg.getMarshaller() != null && !(cfg.getMarshaller() instanceof BinaryMarshaller))
                        return false;

                    if (cfg.isPeerClassLoadingEnabled())
                        return false;

                    return true;
                }
            })
            .skipWaitPartitionMapExchange()
            .gridsCount(4).backups(1)
            .testedNodesCount(2).withClients()
            .classes();
    }
}
