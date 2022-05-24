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
 * Declares ignite::thin::cache::query::ScanQuery class.
 */

#ifndef _IGNITE_THIN_CACHE_QUERY_QUERY_SCAN
#define _IGNITE_THIN_CACHE_QUERY_QUERY_SCAN

#include <stdint.h>
#include <string>
#include <vector>

#include <ignite/impl/thin/copyable_writable.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            // Forward declaration
            class ScanQueryRequest;
        }
    }

    namespace thin
    {
        namespace cache
        {
            namespace query
            {
               /**
                * Scan query.
                */
                class ScanQuery
                {
                public:
                    friend class ScanQueryRequest;

                    /**
                     * Default constructor.
                     */
                    ScanQuery() : part(-1), pageSize(1024), loc(false)
                    {
                        // No-op.
                    }

                    /**
                     * Get partition to scan.
                     *
                     * @return Partition to scan.
                     */
                    int32_t GetPartition() const
                    {
                        return part;
                    }

                    /**
                     * Set partition to scan.
                     *
                     * @param partition Partition to scan.
                     */
                    void SetPartition(int32_t partition)
                    {
                        this->part = partition;
                    }

                    /**
                     * Get page size.
                     *
                     * @return Page size.
                     */
                    int32_t GetPageSize() const
                    {
                        return pageSize;
                    }

                    /**
                     * Set the size of the result page.
                     *
                     * @param resultPageSize Result page size.
                     */
                    void SetPageSize(int32_t resultPageSize)
                    {
                        this->pageSize = resultPageSize;
                    }

                    /**
                     * Get local flag.
                     *
                     * @return Local flag. If true, query will be executed only on the single connected node,
                     *     so only local entries will be returned as query result. Should be used with caution because
                     *     if the client connected to multiple nodes, a single node will be selected randomly.
                     */
                    bool IsLocal() const
                    {
                        return loc;
                    }

                    /**
                     * Set local flag.
                     *
                     * @param localScan Local flag. If true, query will be executed only on the single connected node,
                     *     so only local entries will be returned as query result. Should be used with caution because
                     *     if the client connected to multiple nodes, a single node will be selected randomly.
                     */
                    void SetLocal(bool localScan)
                    {
                        loc = localScan;
                    }

                private:
                    /** Partition. */
                    int32_t part;

                    /** Page size. */
                    int32_t pageSize;

                    /** Local flag. */
                    bool loc;
                };
            }
        }
    }
}

#endif //_IGNITE_THIN_CACHE_QUERY_QUERY_SCAN
