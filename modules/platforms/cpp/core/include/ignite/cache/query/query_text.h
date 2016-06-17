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
 * Declares ignite::cache::query::TextQuery class.
 */

#ifndef _IGNITE_CACHE_QUERY_QUERY_TEXT
#define _IGNITE_CACHE_QUERY_QUERY_TEXT

#include <stdint.h>
#include <string>

#include "ignite/binary/binary_raw_writer.h"

namespace ignite
{    
    namespace cache
    {
        namespace query
        {         
            /**
             * Text query.
             */
            class TextQuery
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param type Type name.
                 * @param text Text string.
                 */
                TextQuery(const std::string& type, const std::string& text) :
                    type(type), text(text), pageSize(1024), loc(false)
                {
                    // No-op.
                }
                
                /**
                 * Get type name.
                 *
                 * @return Type name.
                 */
                const std::string& GetType() const
                {
                    return type;
                }

                /**
                 * Set type name.
                 *
                 * @param sql Type name.
                 */
                void SetType(const std::string& type)
                {
                    this->type = type;
                }

                /**
                 * Get text string.
                 *
                 * @return text string.
                 */
                const std::string& GetText() const
                {
                    return text;
                }

                /**
                 * Set text string.
                 *
                 * @param text Text string.
                 */
                void SetText(const std::string& text)
                {
                    this->text = text;
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
                 * Set page size.
                 *
                 * @param pageSize Page size.
                 */
                void SetPageSize(int32_t pageSize)
                {
                    this->pageSize = pageSize;
                }

                /**
                 * Get local flag.
                 *
                 * @return Local flag.
                 */
                bool IsLocal() const
                {
                    return loc;
                }

                /**
                 * Set local flag.
                 *
                 * @param loc Local flag.
                 */
                void SetLocal(bool loc)
                {
                    this->loc = loc;
                }
                
                /**
                 * Write query info to the stream.
                 *
                 * @param writer Writer.
                 */
                void Write(binary::BinaryRawWriter& writer) const
                {
                    writer.WriteBool(loc);
                    writer.WriteString(text);
                    writer.WriteString(type);
                    writer.WriteInt32(pageSize);
                }

            private:
                /** Type name. */
                std::string type;

                /** Text string. */
                std::string text;

                /** Page size. */
                int32_t pageSize;

                /** Local flag. */
                bool loc;
            };
        }
    }    
}

#endif //_IGNITE_CACHE_QUERY_QUERY_TEXT
