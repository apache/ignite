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
                BinaryOuter();

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
                        one.rawVal1 == two.rawVal1 && one.rawVal2 == two.rawVal2;
                }
            };

            struct PureRaw
            {
                std::string val1;
                int32_t val2;

                PureRaw() : val1(), val2()
                {
                    // No-op.
                }

                PureRaw(std::string val1, int32_t val2) : val1(val1), val2(val2)
                {
                    // No-op.
                }

                friend bool operator==(const PureRaw& one, const PureRaw& two)
                {
                    return one.val1 == two.val1 && one.val2 == two.val2;
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

                virtual BinaryIdResolver* Clone() const
                {
                    return new DummyIdResolver();
                }
            };

            struct TestEnum
            {
                enum Type
                {
                    TEST_ZERO,

                    TEST_NON_ZERO,

                    TEST_NEGATIVE_42 = -42,

                    TEST_SOME_BIG = 1241267
                };
            };

            struct TypeWithEnumField
            {
                int32_t i32Field;
                TestEnum::Type enumField;
                std::string strField;
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
        struct BinaryType<gt::BinaryDummy> : BinaryTypeDefaultAll<gt::BinaryDummy>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "BinaryDummy";
            }

            static void Write(BinaryWriter&, const gt::BinaryDummy&)
            {
                // No-op.
            }

            static void Read(BinaryReader&, gt::BinaryDummy& dst)
            {
                dst = gt::BinaryDummy();
            }
        };

        template<>
        struct BinaryType<gt::BinaryInner> : BinaryTypeDefaultHashing<gt::BinaryInner>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "BinaryInner";
            }

            static bool IsNull(const gt::BinaryInner& obj)
            {
                return obj.GetValue() == 0;
            }

            static void GetNull(gt::BinaryInner& dst)
            {
                dst = gt::BinaryInner(0);
            }

            static void Write(BinaryWriter& writer, const gt::BinaryInner& obj)
            {
                writer.WriteInt32("val", obj.GetValue());
            }

            static void Read(BinaryReader& reader, gt::BinaryInner& dst)
            {
                int val = reader.ReadInt32("val");

                dst = gt::BinaryInner(val);
            }
        };

        template<>
        struct BinaryType<gt::BinaryOuter> : BinaryTypeDefaultHashing<gt::BinaryOuter>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "BinaryOuter";
            }

            static bool IsNull(const gt::BinaryOuter& obj)
            {
                return obj.GetValue() == 0 && obj.GetInner().GetValue();
            }

            static void GetNull(gt::BinaryOuter& dst)
            {
                dst = gt::BinaryOuter(0, 0);
            }

            static void Write(BinaryWriter& writer, const gt::BinaryOuter& obj)
            {
                writer.WriteObject("inner", obj.GetInner());
                writer.WriteInt32("val", obj.GetValue());
            }

            static void Read(BinaryReader& reader, gt::BinaryOuter& dst)
            {
                gt::BinaryInner inner = reader.ReadObject<gt::BinaryInner>("inner");
                int val = reader.ReadInt32("val");

                dst = gt::BinaryOuter(inner.GetValue(), val);
            }
        };

        template<>
        struct BinaryType<gt::BinaryFields> : BinaryTypeDefaultHashing<gt::BinaryFields>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "BinaryFields";
            }

            static bool IsNull(const gt::BinaryFields&)
            {
                return false;
            }

            static void GetNull(gt::BinaryFields&)
            {
                throw std::runtime_error("Must not be called.");
            }

            static void Write(BinaryWriter& writer, const gt::BinaryFields& obj)
            {
                writer.WriteInt32("val1", obj.val1);
                writer.WriteInt32("val2", obj.val2);

                BinaryRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteInt32(obj.rawVal1);
                rawWriter.WriteInt32(obj.rawVal2);
            }

            static void Read(BinaryReader& reader, gt::BinaryFields& dst)
            {
                int32_t val1 = reader.ReadInt32("val1");
                int32_t val2 = reader.ReadInt32("val2");

                BinaryRawReader rawReader = reader.RawReader();

                int32_t rawVal1 = rawReader.ReadInt32();
                int32_t rawVal2 = rawReader.ReadInt32();

                dst = gt::BinaryFields(val1, val2, rawVal1, rawVal2);
            }
        };

        template<>
        struct BinaryType<gt::PureRaw> : BinaryTypeDefaultHashing<gt::PureRaw>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "PureRaw";
            }

            static bool IsNull(const gt::PureRaw&)
            {
                return false;
            }

            static void GetNull(gt::PureRaw&)
            {
                throw std::runtime_error("Must not be called.");
            }

            static void Write(BinaryWriter& writer, const gt::PureRaw& obj)
            {
                BinaryRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteString(obj.val1);
                rawWriter.WriteInt32(obj.val2);
            }

            static void Read(BinaryReader& reader, gt::PureRaw& dst)
            {
                BinaryRawReader rawReader = reader.RawReader();

                dst.val1 = rawReader.ReadString();
                dst.val2 = rawReader.ReadInt32();
            }
        };

        template<>
        struct BinaryEnum<gt::TestEnum::Type> : BinaryEnumDefaultAll<gt::TestEnum::Type>
        {
            /**
             * Get binary object type name.
             *
             * @param dst Output type name.
             */
            static void GetTypeName(std::string& dst)
            {
                dst = "TestEnum";
            }
        };

        template<>
        struct BinaryType<gt::TypeWithEnumField> : BinaryTypeDefaultAll<gt::TypeWithEnumField>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "TypeWithEnumField";
            }

            static void Write(BinaryWriter& writer, const gt::TypeWithEnumField& obj)
            {
                writer.WriteInt32("i32Field", obj.i32Field);
                writer.WriteEnum("enumField", obj.enumField);
                writer.WriteString("strField", obj.strField);
            }

            static void Read(BinaryReader& reader, gt::TypeWithEnumField& dst)
            {
                dst.i32Field = reader.ReadInt32("i32Field");
                dst.enumField = reader.ReadEnum<gt::TestEnum::Type>("enumField");
                dst.strField = reader.ReadString("strField");
            }
        };
    }
}

#endif
