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

#ifndef _IGNITE_IMPL_THIN_CACHE_QUERY_CURSOR_PAGE
#define _IGNITE_IMPL_THIN_CACHE_QUERY_CURSOR_PAGE

#include <ignite/binary/binary_raw_writer.h>

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
                     * Cursor page.
                     */
                    class CursorPage
                    {
                    public:
                        /**
                         * Constructor.
                         */
                        CursorPage() :
                            mem()
                        {
                            // No-op.
                        }

                        /**
                         * Destructor.
                         */
                        virtual ~CursorPage()
                        {
                            // No-op.
                        }

                        /**
                         * Read page using reader.
                         *
                         * @param reader Reader to use.
                         */
                        void Read(binary::BinaryReaderImpl &reader)
                        {
                            interop::InteropInputStream* stream = reader.GetStream();

                            rowNum = reader.ReadInt32();

                            startPos = stream->Position();

                            interop::InteropUnpooledMemory* streamMem =
                                static_cast<interop::InteropUnpooledMemory*>(stream->GetMemory());

                            bool gotOwnership = streamMem->TryGetOwnership(mem);

                            (void) gotOwnership;
                            assert(gotOwnership);
                        }

                        /**
                         * Get row number.
                         *
                         * @return Row number.
                         */
                        int32_t GetRowNum() const
                        {
                            return rowNum;
                        }

                        /**
                         * Get starting position in memory.
                         *
                         * @return Start position.
                         */
                        int32_t GetStartPos() const
                        {
                            return startPos;
                        }

                        /**
                         * Get memory.
                         *
                         * @return Page memory.
                         */
                        interop::InteropUnpooledMemory* GetMemory()
                        {
                            return &mem;
                        }

                    private:
                        /** Row Number. */
                        int32_t rowNum;

                        /** Start position. */
                        int32_t startPos;

                        /** Page memory. */
                        interop::InteropUnpooledMemory mem;
                    };

                    /** Cursor page shared pointer. */
                    typedef common::concurrent::SharedPointer<CursorPage> SP_CursorPage;
                }
            }
        }
    }
}

#endif // _IGNITE_IMPL_THIN_CACHE_QUERY_CURSOR_PAGE
