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

#ifndef _IGNITE_PORTABLE_TEST_DEFS
#define _IGNITE_PORTABLE_TEST_DEFS

#include <stdexcept>
#include <stdint.h>

#include "ignite/portable/portable.h"

namespace ignite_test
{
    namespace core
    {
        namespace portable 
        {
            class PortableDummy
            {
                // No-op.
            };

            class PortableInner 
            {
            public:
                PortableInner();

                PortableInner(int32_t val);

                int32_t GetValue() const;
            private:
                int32_t val;
            };

            class PortableOuter
            {
            public:
                PortableOuter(int32_t valIn, int32_t valOut);

                PortableInner GetInner() const;

                int32_t GetValue() const;
            private:
                PortableInner inner;
                int32_t val;
            };

            struct PortableFields
            {
                int32_t val1;
                int32_t val2;
                int32_t rawVal1;
                int32_t rawVal2;

                PortableFields() : val1(0), val2(0), rawVal1(0), rawVal2(0)
                {
                    // No-op.
                }

                PortableFields(int32_t val1, int32_t val2, int32_t rawVal1, int32_t rawVal2) :
                    val1(val1), val2(val2), rawVal1(rawVal1), rawVal2(rawVal2)
                {
                    // No-op.   
                }
            };
        }
    }
}

namespace ignite
{
    namespace portable
    {
        namespace gt = ignite_test::core::portable;

        template<>
        struct PortableType<gt::PortableDummy>
        {
            /** <inheritdoc /> */
            int32_t GetTypeId()
            {
                return GetPortableStringHashCode("PortableDummy");
            }

            /** <inheritdoc /> */
            std::string GetTypeName()
            {
                return "PortableDummy";
            }

            /** <inheritdoc /> */
            int32_t GetFieldId(const char* name)
            {
                return GetPortableStringHashCode(name);
            }

            /** <inheritdoc /> */
            int32_t GetHashCode(const gt::PortableInner& obj)
            {
                return obj.GetValue();
            }

            /** <inheritdoc /> */
            bool IsNull(const gt::PortableInner& obj)
            {
                return obj.GetValue() == 0;
            }

            /** <inheritdoc /> */
            gt::PortableInner GetNull()
            {
                return gt::PortableInner(0);
            }

            /** <inheritdoc /> */
            void Write(PortableWriter& writer, const gt::PortableDummy& obj)
            {
                // No-op.
            }

            /** <inheritdoc /> */
            gt::PortableDummy Read(PortableReader& reader)
            {
                return gt::PortableDummy();
            }
        };

        template<> 
        struct PortableType<gt::PortableInner>
        {
            /** <inheritdoc /> */
            int32_t GetTypeId() 
            { 
                return GetPortableStringHashCode("PortableInner"); 
            }

            /** <inheritdoc /> */
            std::string GetTypeName()
            {
                return "PortableInner";
            }

            /** <inheritdoc /> */
            int32_t GetFieldId(const char* name) 
            { 
                return GetPortableStringHashCode(name); 
            }

            /** <inheritdoc /> */
            int32_t GetHashCode(const gt::PortableInner& obj)
            {
                return obj.GetValue();
            }

            /** <inheritdoc /> */
            bool IsNull(const gt::PortableInner& obj)
            {
                return obj.GetValue() == 0;
            }

            /** <inheritdoc /> */
            gt::PortableInner GetNull()
            {
                return gt::PortableInner(0);
            }

            /** <inheritdoc /> */
            void Write(PortableWriter& writer, const gt::PortableInner& obj)
            {
                writer.WriteInt32("val", obj.GetValue());
            }

            /** <inheritdoc /> */
            gt::PortableInner Read(PortableReader& reader)
            {
                int val = reader.ReadInt32("val");

                return gt::PortableInner(val);
            }
        };

        template<>
        struct PortableType<gt::PortableOuter>
        {
            /** <inheritdoc /> */
            int32_t GetTypeId()
            {
                return GetPortableStringHashCode("PortableOuter");
            }

            /** <inheritdoc /> */
            std::string GetTypeName()
            {
                return "PortableOuter";
            }

            /** <inheritdoc /> */
            int32_t GetFieldId(const char* name)
            {
                return GetPortableStringHashCode(name);
            }

            /** <inheritdoc /> */
            int32_t GetHashCode(const gt::PortableOuter& obj)
            {
                return obj.GetValue() + obj.GetInner().GetValue();
            }

            /** <inheritdoc /> */
            bool IsNull(const gt::PortableOuter& obj)
            {
                return obj.GetValue() == 0 && obj.GetInner().GetValue();
            }

            /** <inheritdoc /> */
            gt::PortableOuter GetNull()
            {
                return gt::PortableOuter(0, 0);
            }

            /** <inheritdoc /> */
            void Write(PortableWriter& writer, const gt::PortableOuter& obj)
            {
                writer.WriteObject("inner", obj.GetInner());
                writer.WriteInt32("val", obj.GetValue());                
            }

            /** <inheritdoc /> */
            gt::PortableOuter Read(PortableReader& reader)
            {
                gt::PortableInner inner = reader.ReadObject<gt::PortableInner>("inner");
                int val = reader.ReadInt32("val");

                return gt::PortableOuter(inner.GetValue(), val);
            }
        };

        template<>
        struct PortableType<gt::PortableFields>
        {
            /** <inheritdoc /> */
            int32_t GetTypeId()
            {
                return GetPortableStringHashCode("PortableFields");
            }

            /** <inheritdoc /> */
            std::string GetTypeName()
            {
                return "PortableFields";
            }

            /** <inheritdoc /> */
            int32_t GetFieldId(const char* name)
            {
                return GetPortableStringHashCode(name);
            }

            /** <inheritdoc /> */
            int32_t GetHashCode(const gt::PortableFields& obj)
            {
                return obj.val1 + obj.val2 + obj.rawVal1 + obj.rawVal2;
            }

            /** <inheritdoc /> */
            bool IsNull(const gt::PortableFields& obj)
            {
                return false;
            }

            /** <inheritdoc /> */
            gt::PortableFields GetNull()
            {
                throw std::runtime_error("Must not be called.");
            }

            /** <inheritdoc /> */
            void Write(PortableWriter& writer, const gt::PortableFields& obj)
            {
                writer.WriteInt32("val1", obj.val1);
                writer.WriteInt32("val2", obj.val2);

                PortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteInt32(obj.rawVal1);
                rawWriter.WriteInt32(obj.rawVal2);
            }

            /** <inheritdoc /> */
            gt::PortableFields Read(PortableReader& reader)
            {
                int32_t val1 = reader.ReadInt32("val1");
                int32_t val2 = reader.ReadInt32("val2");

                PortableRawReader rawReader = reader.RawReader();

                int32_t rawVal1 = rawReader.ReadInt32();
                int32_t rawVal2 = rawReader.ReadInt32();

                return gt::PortableFields(val1, val2, rawVal1, rawVal2);
            }
        };
    }
}

#endif