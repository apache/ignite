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

/**
 * @file
 * Declares ignite::thin::cache::CacheClientConfiguration.
 */

#ifndef _IGNITE_THIN_CACHE_CACHE_CLIENT_CONFIGURATION
#define _IGNITE_THIN_CACHE_CACHE_CLIENT_CONFIGURATION

namespace ignite
{
    namespace thin
    {
        namespace cache
        {
            /**
             * Cache client class.
             *
             * Used to configure CacheClient.
             */
            class CacheClientConfiguration
            {
            public:
                /**
                 * Constructor.
                 *
                 * Constructs configuration with all parameters set to default values.
                 */
                CacheClientConfiguration() :
                    lessenLatency(false)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~CacheClientConfiguration()
                {
                    // No-op.
                }

                /**
                 * Enable "lessen latency" feature.
                 *
                 * Disabled by default.
                 *
                 * When disabled, client sends requests to random nodes from the list, provided by user.
                 * When enabled, client uses RendezvousAffinityFunction algorithm to find out the bets node, to send
                 * data to. This may help lessen latency and increase throughput. For now, only can be used with caches,
                 * that use RendezvousAffinityFunction.
                 *
                 * @param lessenLatency Lessen latency flag.
                 */
                void SetLessenLatency(bool lessenLatency)
                {
                    this->lessenLatency = lessenLatency;
                }

                /**
                 * Check lessen latency flag.
                 *
                 * @see SetLessenLatency for details.
                 *
                 * @return Lessen latency flag.
                 */
                bool IsLessenLatency() const
                {
                    return lessenLatency;
                }

            private:
                /** Lessen latency flag. */
                bool lessenLatency;
            };
        }
    }
}

#endif // _IGNITE_THIN_CACHE_CACHE_CLIENT_CONFIGURATION
