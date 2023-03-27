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

#ifndef _IGNITE_IMPL_THIN_CACHE_AFFINITY_KEY
#define _IGNITE_IMPL_THIN_CACHE_AFFINITY_KEY

#include <stdint.h>
#include <vector>

#include <ignite/impl/binary/binary_reader_impl.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Affinity configuration.
             */
            class AffinityConfiguration
            {
            public:
                /**
                 * Default constructor.
                 */
                AffinityConfiguration() :
                    keyTypeId(0),
                    keyAffinityFieldId(0)
                {
                    // No-op.
                }

                /**
                 * Get key type ID.
                 *
                 * @return Type ID of the key.
                 */
                int32_t GetKeyTypeId() const
                {
                    return keyTypeId;
                }

                /**
                 * Get affinity field ID of the key.
                 *
                 * @return Affinity field of the key.
                 */
                int32_t GetKeyAffinityFieldId() const
                {
                    return keyAffinityFieldId;
                }

                /**
                 * Read from data stream, using provided reader.
                 *
                 * @param reader Reader.
                 */
                void Read(binary::BinaryReaderImpl& reader)
                {
                    keyTypeId = reader.ReadInt32();
                    keyAffinityFieldId = reader.ReadInt32();
                }

            private:
                /** Type ID of the key. */
                int32_t keyTypeId;

                /** Affinity field of the key. */
                int32_t keyAffinityFieldId;
            };

            /**
             * Affinity configurations associated with the cache.
             */
            class CacheAffinityConfigs
            {
            public:
                /**
                 * Default constructor.
                 */
                CacheAffinityConfigs() :
                    cacheId(0)
                {
                    // No-op.
                }
                
                /**
                 * Get cache ID.
                 *
                 * @return Cache ID.
                 */
                int32_t GetCacheId() const
                {
                    return cacheId;
                }

                /**
                 * Get affinity configurations of the cache.
                 *
                 * @return Affinity configurations.
                 */
                const std::vector<AffinityConfiguration>& GetAffinityConfigs() const
                {
                    return affinityConfigs;
                }

                /**
                 * Read from data stream, using provided reader.
                 *
                 * @param reader Reader.
                 * @param hasKeyMeta Indicates whether key meta is available in stream.
                 */
                void Read(binary::BinaryReaderImpl& reader, bool hasKeyMeta)
                {
                    affinityConfigs.clear();

                    cacheId = reader.ReadInt32();

                    if (hasKeyMeta)
                    {
                        int32_t configsNum = reader.ReadInt32();

                        affinityConfigs.resize(static_cast<size_t>(configsNum));

                        for (int i = 0; i < configsNum; ++i)
                            affinityConfigs[i].Read(reader);
                    }
                }

            private:
                /** Cache ID. */
                int32_t cacheId;

                /** Affinity configurations of the cache. */
                std::vector<AffinityConfiguration> affinityConfigs;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_CACHE_AFFINITY_KEY