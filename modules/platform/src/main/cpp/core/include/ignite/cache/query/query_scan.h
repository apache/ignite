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

#ifndef _IGNITE_CACHE_QUERY_SCAN
#define _IGNITE_CACHE_QUERY_SCAN

#include <stdint.h>
#include <string>

#include "ignite/portable/portable_raw_writer.h"

namespace ignite
{    
    namespace cache
    {
        namespace query
        {         
            /*
             * Scab query.
             */
            class ScanQuery
            {
            public:
                /* 
                 * Constructor.
                 */
                ScanQuery() : part(-1), pageSize(1024), loc(false)
                {
                    // No-op.
                }
                
                /*
                 * Constructor.
                 *
                 * @param part Partition.
                 */
                ScanQuery(int32_t part) : part(part), pageSize(1024), loc(false)
                {
                    // No-op.
                }
                
                /*
                 * Get partition to scan.
                 *
                 * @return Partition to scan.
                 */
                int32_t GetPartition()
                {
                    return part;
                }

                /*
                 * Set partition to scan.
                 *
                 * @param part Partition to scan.
                 */
                void SetPartition(int32_t part)
                {
                    this->part = part;
                }

                /*
                 * Get page size.
                 *
                 * @return Page size.
                 */
                int32_t GetPageSize()
                {
                    return pageSize;
                }

                /*
                 * Set page size.
                 *
                 * @param pageSize Page size.
                 */
                void SetPageSize(int32_t pageSize)
                {
                    this->pageSize = pageSize;
                }

                /*
                 * Get local flag.
                 *
                 * @return Local flag.
                 */
                bool IsLocal()
                {
                    return loc;
                }

                /*
                 * Set local flag.
                 *
                 * @param loc Local flag.
                 */
                void SetLocal(bool loc)
                {
                    this->loc = loc;
                }
                
                /*
                 * Write query info to the stream.
                 *
                 * @param writer Writer.
                 */
                void Write(portable::PortableRawWriter& writer) const
                {
                    writer.WriteBool(loc);
                    writer.WriteInt32(pageSize);

                    if (part < 0)
                        writer.WriteBool(false);
                    else
                    {
                        writer.WriteBool(true);
                        writer.WriteInt32(part);
                    }

                    writer.WriteNull(); // Predicates are not supported yet.
                }

            private:
                /* Partition. */
                int32_t part;

                /* Page size. */
                int32_t pageSize;

                /* Local flag. */
                bool loc;
            };
        }
    }    
}

#endif