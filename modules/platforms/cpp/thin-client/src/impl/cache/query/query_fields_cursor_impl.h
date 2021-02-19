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

#ifndef _IGNITE_IMPL_THIN_CACHE_QUERY_QUERY_FIELDS_CURSOR_IMPL
#define _IGNITE_IMPL_THIN_CACHE_QUERY_QUERY_FIELDS_CURSOR_IMPL

#include <ignite/common/concurrent.h>

#include <ignite/thin/cache/query/query_fields_row.h>

#include "impl/cache/query/cursor_page.h"
#include "impl/cache/query/query_fields_row_impl.h"
#include "impl/data_router.h"
#include "impl/message.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                namespace query
                {
                    /**
                     * Query Fields Cursor Implementation.
                     */
                    class QueryFieldsCursorImpl
                    {
                    public:
                        /**
                         * Constructor.
                         *
                         * @param id Cursor ID.
                         * @param columns Column names.
                         * @param cursorPage Cursor page.
                         * @param channel Data channel. Used to request new page.
                         * @param timeout Timeout.
                         */
                        QueryFieldsCursorImpl(
                                int64_t id,
                                const std::vector<std::string>& columns,
                                const SP_CursorPage &cursorPage,
                                const SP_DataChannel& channel,
                                int32_t timeout) :
                            id(id),
                            columns(columns),
                            page(cursorPage),
                            channel(channel),
                            timeout(timeout),
                            currentRow(0),
                            stream(page.Get()->GetMemory()),
                            reader(&stream),
                            endReached(false)
                        {
                            stream.Position(page.Get()->GetStartPos());

                            CheckEnd();
                        }

                        /**
                         * Destructor.
                         */
                        virtual ~QueryFieldsCursorImpl()
                        {
                            // No-op.
                        }

                        /**
                         * Check whether next entry exists.
                         *
                         * @return @c true if next entry exists.
                         *
                         * @throw IgniteError class instance in case of failure.
                         */
                        bool HasNext()
                        {
                            return !endReached;
                        }

                        /**
                         * Get next entry.
                         *
                         * This method should only be used on the valid instance.
                         *
                         * @return Next entry.
                         *
                         * @throw IgniteError class instance in case of failure.
                         */
                        ignite::thin::cache::query::QueryFieldsRow GetNext()
                        {
                            if (!HasNext())
                                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "The cursor is empty");

                            if (IsUpdateNeeded())
                                Update();

                            SP_QueryFieldsRowImpl rowImpl(
                                new QueryFieldsRowImpl(
                                        static_cast<int32_t>(columns.size()),
                                        page,
                                        stream.Position()));

                            SkipRow();

                            return ignite::thin::cache::query::QueryFieldsRow(rowImpl);
                        }

                        /**
                         * Get column names.
                         *
                         * @return Column names.
                         */
                        const std::vector<std::string>& GetColumns() const
                        {
                            return columns;
                        }

                    private:
                        /**
                         * Check whether next page should be retrieved from the server.
                         *
                         * @return @c true if next page should be fetched.
                         */
                        bool IsUpdateNeeded()
                        {
                            return !page.IsValid() && !endReached;
                        }

                        /**
                         * Fetch next cursor page.
                         */
                        void Update()
                        {
                            SqlFieldsCursorGetPageRequest req(id);
                            SqlFieldsCursorGetPageResponse rsp;

                            DataChannel* channel0 = channel.Get();

                            if (!channel0)
                                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Connection is not established");

                            channel0->SyncMessage(req, rsp, timeout);

                            page = rsp.GetCursorPage();
                            currentRow = 0;

                            stream = interop::InteropInputStream(page.Get()->GetMemory());
                            stream.Position(page.Get()->GetStartPos());
                        }

                        /**
                         * Skip position to the next row.
                         */
                        void SkipRow()
                        {
                            for (size_t i = 0; i < columns.size(); ++i)
                                reader.Skip();

                            ++currentRow;

                            CheckEnd();
                        }

                        /**
                         * Check whether end is reached.
                         */
                        void CheckEnd()
                        {
                            if (currentRow == page.Get()->GetRowNum())
                            {
                                bool hasNextPage = reader.ReadBool();
                                endReached = !hasNextPage;

                                page = SP_CursorPage();
                            }
                        }

                        /** Cursor ID. */
                        int64_t id;

                        /** Column names. */
                        std::vector<std::string> columns;

                        /** Cursor page. */
                        SP_CursorPage page;

                        /** Data channel. */
                        SP_DataChannel channel;

                        /** Timeout in milliseconds. */
                        int32_t timeout;

                        /** Current row in page. */
                        int32_t currentRow;

                        /** Stream. */
                        interop::InteropInputStream stream;

                        /** Reader. */
                        binary::BinaryReaderImpl reader;

                        /** End reached. */
                        bool endReached;
                    };

                    typedef common::concurrent::SharedPointer<QueryFieldsCursorImpl> SP_QueryFieldsCursorImpl;
                }
            }
        }
    }
}

#endif // _IGNITE_IMPL_THIN_CACHE_QUERY_QUERY_FIELDS_CURSOR_IMPL
