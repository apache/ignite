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

#include "impl/protocol_context.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            const ProtocolVersion VERSION_1_2_0(1, 2, 0);
            const ProtocolVersion VERSION_1_3_0(1, 3, 0);
            const ProtocolVersion VERSION_1_4_0(1, 4, 0);
            const ProtocolVersion VERSION_1_5_0(1, 5, 0);
            const ProtocolVersion VERSION_1_6_0(1, 6, 0);
            const ProtocolVersion VERSION_1_7_0(1, 7, 0);

            VersionSet::value_type supportedArray[] = {
                VERSION_1_7_0,
                VERSION_1_6_0,
                VERSION_1_5_0,
                VERSION_1_4_0,
                VERSION_1_3_0,
                VERSION_1_2_0,
            };

            const VersionSet supportedVersions(supportedArray,
                supportedArray + (sizeof(supportedArray) / sizeof(supportedArray[0])));

            VersionFeature::Type VersionFeature::PARTITION_AWARENESS(VERSION_1_4_0);
            VersionFeature::Type VersionFeature::BITMAP_FEATURES(VERSION_1_7_0);

            const ProtocolVersion ProtocolContext::VERSION_LATEST(VERSION_1_7_0);

            ProtocolContext::ProtocolContext() :
                ver(VERSION_LATEST),
                features()
            {
                features.set(BitmaskFeature::QRY_PARTITIONS_BATCH_SIZE);
            }

            ProtocolContext::ProtocolContext(const ProtocolVersion& ver) :
                ver(ver),
                features()
            {
                if (IsFeatureSupported(VersionFeature::BITMAP_FEATURES))
                    features.set(BitmaskFeature::QRY_PARTITIONS_BATCH_SIZE);
            }

            bool ProtocolContext::IsFeatureSupported(BitmaskFeature::Type feature) const
            {
                if (feature >= BitmaskFeature::MAX_SUPPORTED)
                    return false;

                return features.test(feature);
            }

            bool ProtocolContext::IsVersionSupported(const ProtocolVersion& ver)
            {
                return supportedVersions.find(ver) != supportedVersions.end();
            }
        }
    }
}

