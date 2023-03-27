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
  * Declares ignite::IgniteProductVersion class.
  */

#ifndef _IGNITE_IGNITE_PRODUCT_VERSION
#define _IGNITE_IGNITE_PRODUCT_VERSION

#include <stdint.h>
#include <vector>

#include <ignite/ignite_error.h>

namespace ignite
{
    /**
     * %Ignite product version.
     */
    struct IgniteProductVersion
    {
        /** Major version number. */
        int8_t majorNumber;

        /** Minor version number. */
        int8_t minorNumber;

        /** Maintenance version number. */
        int8_t maintenance;

        /** Stage of development. */
        std::string stage;

        /** Release date. */
        int64_t releaseDate;

        /** Revision hash. */
        std::vector<int8_t> revHash;

        /** SHA1 Length. */
        static const int SHA1_LENGTH = 20;

        /**
         * Default constructor.
         */
        IgniteProductVersion(int8_t majorNumber, int8_t minorNumber, int8_t maintenance, std::string stage, int64_t releaseDate, std::vector<int8_t> revHash) :
            majorNumber(majorNumber), minorNumber(minorNumber), maintenance(maintenance), stage(stage), releaseDate(releaseDate), revHash(revHash)
        {
            assert(revHash.size() == SHA1_LENGTH);
        }
    };
}

#endif //_IGNITE_IGNITE_PRODUCT_VERSION