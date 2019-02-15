/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

/**
 * @file
 * Declares ignite::cache::query::QueryArgument class template and
 * ignite::cache::query::QueryArgumentBase interface.
 */

#ifndef _IGNITE_IMPL_CACHE_QUERY_QUERY_ARGUMENT
#define _IGNITE_IMPL_CACHE_QUERY_QUERY_ARGUMENT

#include <ignite/binary/binary_raw_writer.h>

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            namespace query
            {
                /**
                 * Base class for all query arguments.
                 */
                class QueryArgumentBase
                {
                public:
                    /**
                     * Destructor.
                     */
                    virtual ~QueryArgumentBase()
                    {
                        // No-op.
                    }

                    /**
                     * Copy argument.
                     *
                     * @return Copy of this argument instance.
                     */
                    virtual QueryArgumentBase* Copy() const = 0;

                    /**
                     * Write argument using provided writer.
                     *
                     * @param writer Writer to use to write this argument.
                     */
                    virtual void Write(ignite::binary::BinaryRawWriter& writer) = 0;
                };

                /**
                 * Query argument class template.
                 *
                 * Template argument type should be copy-constructable and
                 * assignable. Also BinaryType class template should be specialized
                 * for this type.
                 */
                template<typename T>
                class QueryArgument : public QueryArgumentBase
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param val Value.
                     */
                    QueryArgument(const T& val) :
                        val(val)
                    {
                        // No-op.
                    }

                    /**
                     * Copy constructor.
                     *
                     * @param other Other instance.
                     */
                    QueryArgument(const QueryArgument& other) :
                        val(other.val)
                    {
                        // No-op.
                    }

                    /**
                     * Assignment operator.
                     *
                     * @param other Other instance.
                     * @return *this.
                     */
                    QueryArgument& operator=(const QueryArgument& other)
                    {
                        if (this != &other)
                            val = other.val;

                        return *this;
                    }

                    virtual ~QueryArgument()
                    {
                        // No-op.
                    }

                    virtual QueryArgumentBase* Copy() const
                    {
                        return new QueryArgument(val);
                    }

                    virtual void Write(ignite::binary::BinaryRawWriter& writer)
                    {
                        writer.WriteObject<T>(val);
                    }

                private:
                    /** Value. */
                    T val;
                };

                /**
                 * Query bytes array argument class.
                 */
                class QueryInt8ArrayArgument : public QueryArgumentBase
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param src Array.
                     * @param len Array length.
                     */
                    QueryInt8ArrayArgument(const int8_t* src, int32_t len) :
                        val(src, src + len)
                    {
                        // No-op.
                    }

                    /**
                     * Copy constructor.
                     *
                     * @param other Other instance.
                     */
                    QueryInt8ArrayArgument(const QueryInt8ArrayArgument& other) :
                        val(other.val)
                    {
                        // No-op.
                    }

                    /**
                     * Assignment operator.
                     *
                     * @param other Other instance.
                     * @return *this.
                     */
                    QueryInt8ArrayArgument& operator=(const QueryInt8ArrayArgument& other)
                    {
                        if (this != &other)
                            val = other.val;

                        return *this;
                    }

                    virtual ~QueryInt8ArrayArgument()
                    {
                        // No-op.
                    }

                    virtual QueryArgumentBase* Copy() const
                    {
                        return new QueryInt8ArrayArgument(*this);
                    }

                    virtual void Write(ignite::binary::BinaryRawWriter& writer)
                    {
                        writer.WriteInt8Array(&val[0], static_cast<int32_t>(val.size()));
                    }

                private:
                    /** Value. */
                    std::vector<int8_t> val;
                };
            }
        }
    }
}

#endif //_IGNITE_IMPL_CACHE_QUERY_QUERY_ARGUMENT