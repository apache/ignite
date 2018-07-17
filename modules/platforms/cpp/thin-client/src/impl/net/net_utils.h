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

#ifndef _IGNITE_IMPL_THIN_NET_NET_UTILS
#define _IGNITE_IMPL_THIN_NET_NET_UTILS

#include <set>
#include <string>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace net
            {
                namespace net_utils
                {
                    /**
                     * Get set of local addresses.
                     *
                     * @param addrs Addresses set.
                     */
                    void GetLocalAddresses(std::set<std::string>& addrs);
                }
            }
        }
    }
}

#endif //_IGNITE_IMPL_THIN_NET_NET_UTILS