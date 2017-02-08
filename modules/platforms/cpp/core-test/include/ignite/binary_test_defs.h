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

#ifndef _IGNITE_BINARY_TEST_DEFS
#define _IGNITE_BINARY_TEST_DEFS

#include <stdexcept>
#include <stdint.h>

#include "ignite/binary/binary.h"

namespace ignite_test
{
    namespace core
    {
        namespace binary 
        {
            class BinaryDummy
            {
                // No-op.
            };

            class BinaryInner 
            {
            public:
                BinaryInner();

                BinaryInner(int32_t val);

                int32_t GetValue() const;
            private:
                int32_t val;
            };

            class BinaryOuter
            {
            public:
                BinaryOuter(int32_t valIn, int32_t valOut);

                BinaryInner GetInner() const;

                int32_t GetValue() const;
            private:
                BinaryInner inner;
                int32_t val;
            };

            struct BinaryFields
            {
                int32_t val1;
                int32_t val2;
                int32_t rawVal1;
                int32_t rawVal2;

                BinaryFields() : val1(0), val2(0), rawVal1(0), rawVal2(0)
                {
                    // No-op.
                }

                BinaryFields(int32_t val1, int32_t val2, int32_t rawVal1, int32_t rawVal2) :
                    val1(val1), val2(val2), rawVal1(rawVal1), rawVal2(rawVal2)
                {
                    // No-op.   
                }

                friend bool operator==(const BinaryFields& one, const BinaryFields& two)
                {
                    return one.val1 == two.val1 && one.val2 == two.val2 && 
                        one.rawVal1 == two.rawVal1 &&one.rawVal2 == two.rawVal2;
                }
            };

            class DummyIdResolver : public ignite::impl::binary::BinaryIdResolver
            {
            public:
                virtual ~DummyIdResolver()
                {
                    // No-op.
                }

                virtual int32_t GetTypeId()
                {
                    return 0;
                }

                virtual int32_t GetFieldId(const int32_t, const char*)
                {
                    return 0;
                }
            };
        }
    }
}

namespace ignite
{
    namespace binary
    {
        namespace gt = ignite_test::core::binary;

        template<>
        struct BinaryType<gt::BinaryDummy>
        {
            /** <inheritdoc /> */
            int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("BinaryDummy");
            }

            /** <inheritdoc /> */
            std::string GetTypeName()
            {
                return "BinaryDummy";
            }

            /** <inheritdoc /> */
            int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            /** <inheritdoc /> */
            int32_t GetHashCode(const gt::BinaryInner& obj)
            {
                return obj.GetValue();
            }

            /** <inheritdoc /> */
            bool IsNull(const gt::BinaryInner& obj)
            {
                return obj.GetValue() == 0;
            }

            /** <inheritdoc /> */
            gt::BinaryInner GetNull()
            {
                return gt::BinaryInner(0);
            }

            /** <inheritdoc /> */
            void Write(BinaryWriter& writer, const gt::BinaryDummy& obj)
            {
                // No-op.
            }

            /** <inheritdoc /> */
            gt::BinaryDummy Read(BinaryReader& reader)
            {
                return gt::BinaryDummy();
            }
        };

        template<> 
        struct BinaryType<gt::BinaryInner>
        {
            /** <inheritdoc /> */
            int32_t GetTypeId() 
            { 
                return GetBinaryStringHashCode("BinaryInner"); 
            }

            /** <inheritdoc /> */
            std::string GetTypeName()
            {
                return "BinaryInner";
            }

            /** <inheritdoc /> */
            int32_t GetFieldId(const char* name) 
            { 
                return GetBinaryStringHashCode(name); 
            }

            /** <inheritdoc /> */
            int32_t GetHashCode(const gt::BinaryInner& obj)
            {
                return obj.GetValue();
            }

            /** <inheritdoc /> */
            bool IsNull(const gt::BinaryInner& obj)
            {
                return obj.GetValue() == 0;
            }

            /** <inheritdoc /> */
            gt::BinaryInner GetNull()
            {
                return gt::BinaryInner(0);
            }

            /** <inheritdoc /> */
            void Write(BinaryWriter& writer, const gt::BinaryInner& obj)
            {
                writer.WriteInt32("val", obj.GetValue());
            }

            /** <inheritdoc /> */
            gt::BinaryInner Read(BinaryReader& reader)
            {
                int val = reader.ReadInt32("val");

                return gt::BinaryInner(val);
            }
        };

        template<>
        struct BinaryType<gt::BinaryOuter>
        {
            /** <inheritdoc /> */
            int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("BinaryOuter");
            }

            /** <inheritdoc /> */
            std::string GetTypeName()
            {
                return "BinaryOuter";
            }

            /** <inheritdoc /> */
            int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            /** <inheritdoc /> */
            int32_t GetHashCode(const gt::BinaryOuter& obj)
            {
                return obj.GetValue() + obj.GetInner().GetValue();
            }

            /** <inheritdoc /> */
            bool IsNull(const gt::BinaryOuter& obj)
            {
                return obj.GetValue() == 0 && obj.GetInner().GetValue();
            }

            /** <inheritdoc /> */
            gt::BinaryOuter GetNull()
            {
                return gt::BinaryOuter(0, 0);
            }

            /** <inheritdoc /> */
            void Write(BinaryWriter& writer, const gt::BinaryOuter& obj)
            {
                writer.WriteObject("inner", obj.GetInner());
                writer.WriteInt32("val", obj.GetValue());                
            }

            /** <inheritdoc /> */
            gt::BinaryOuter Read(BinaryReader& reader)
            {
                gt::BinaryInner inner = reader.ReadObject<gt::BinaryInner>("inner");
                int val = reader.ReadInt32("val");

                return gt::BinaryOuter(inner.GetValue(), val);
            }
        };

        template<>
        struct BinaryType<gt::BinaryFields>
        {
            /** <inheritdoc /> */
            int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("BinaryFields");
            }

            /** <inheritdoc /> */
            std::string GetTypeName()
            {
                return "BinaryFields";
            }

            /** <inheritdoc /> */
            int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            /** <inheritdoc /> */
            int32_t GetHashCode(const gt::BinaryFields& obj)
            {
                return obj.val1 + obj.val2 + obj.rawVal1 + obj.rawVal2;
            }

            /** <inheritdoc /> */
            bool IsNull(const gt::BinaryFields& obj)
            {
                return false;
            }

            /** <inheritdoc /> */
            gt::BinaryFields GetNull()
            {
                throw std::runtime_error("Must not be called.");
            }

            /** <inheritdoc /> */
            void Write(BinaryWriter& writer, const gt::BinaryFields& obj)
            {
                writer.WriteInt32("val1", obj.val1);
                writer.WriteInt32("val2", obj.val2);

                BinaryRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteInt32(obj.rawVal1);
                rawWriter.WriteInt32(obj.rawVal2);
            }

            /** <inheritdoc /> */
            gt::BinaryFields Read(BinaryReader& reader)
            {
                int32_t val1 = reader.ReadInt32("val1");
                int32_t val2 = reader.ReadInt32("val2");

                BinaryRawReader rawReader = reader.RawReader();

                int32_t rawVal1 = rawReader.ReadInt32();
                int32_t rawVal2 = rawReader.ReadInt32();

                return gt::BinaryFields(val1, val2, rawVal1, rawVal2);
            }
        };
    }
}

#endif