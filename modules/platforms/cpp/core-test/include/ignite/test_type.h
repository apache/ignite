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

#ifndef _IGNITE_ODBC_TEST_TEST_TYPE
#define _IGNITE_ODBC_TEST_TEST_TYPE

#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

namespace ignite
{
    struct TestType
    {
        TestType() :
            allNulls(false),
            i8Field(0),
            i16Field(0),
            i32Field(0),
            i64Field(0),
            floatField(0.0f),
            doubleField(0.0),
            boolField(false),
            dateField(),
            timestampField()
        {
            // No-op.
        }

        TestType(int8_t i8Field, int16_t i16Field, int32_t i32Field,
            int64_t i64Field, const std::string& strField, float floatField,
            double doubleField, bool boolField, const Guid& guidField,
            const Date& dateField, const Timestamp& timestampField) :
            allNulls(false),
            i8Field(i8Field),
            i16Field(i16Field),
            i32Field(i32Field),
            i64Field(i64Field),
            strField(strField),
            floatField(floatField),
            doubleField(doubleField),
            boolField(boolField),
            guidField(guidField),
            dateField(dateField),
            timestampField(timestampField)
        {
            // No-op.
        }

        friend bool operator==(const TestType& one, const TestType& two)
        {
            return
                one.allNulls == two.allNulls &&
                one.i8Field == two.i8Field &&
                one.i16Field == two.i16Field &&
                one.i32Field == two.i32Field &&
                one.i64Field == two.i64Field &&
                one.strField == two.strField &&
                one.floatField == two.floatField &&
                one.doubleField == two.doubleField &&
                one.boolField == two.boolField &&
                one.guidField == two.guidField &&
                one.dateField == two.dateField &&
                one.timestampField == two.timestampField &&
                one.i8ArrayField == two.i8ArrayField;
        }

        bool allNulls;
        int8_t i8Field;
        int16_t i16Field;
        int32_t i32Field;
        int64_t i64Field;
        std::string strField;
        float floatField;
        double doubleField;
        bool boolField;
        Guid guidField;
        Date dateField;
        Timestamp timestampField;
        std::vector<int8_t> i8ArrayField;
    };
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::TestType)

            typedef ignite::TestType TestType;

            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(TestType)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(TestType)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(TestType)
            IGNITE_BINARY_IS_NULL_FALSE(TestType)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(TestType)

            void Write(BinaryWriter& writer, TestType obj)
            {
                if (!obj.allNulls)
                {
                    writer.WriteInt8("i8Field", obj.i8Field);
                    writer.WriteInt16("i16Field", obj.i16Field);
                    writer.WriteInt32("i32Field", obj.i32Field);
                    writer.WriteInt64("i64Field", obj.i64Field);
                    writer.WriteString("strField", obj.strField);
                    writer.WriteFloat("floatField", obj.floatField);
                    writer.WriteDouble("doubleField", obj.doubleField);
                    writer.WriteBool("boolField", obj.boolField);
                    writer.WriteGuid("guidField", obj.guidField);
                    writer.WriteDate("dateField", obj.dateField);
                    writer.WriteTimestamp("timestampField", obj.timestampField);
                    if (obj.i8ArrayField.empty())
                    {
                        writer.WriteNull("i8ArrayField");
                    }
                    else
                    {
                        writer.WriteInt8Array("i8ArrayField", &obj.i8ArrayField[0], static_cast<int32_t>(obj.i8ArrayField.size()));
                    }
                }
                else
                {
                    writer.WriteNull("i8Field");
                    writer.WriteNull("i16Field");
                    writer.WriteNull("i32Field");
                    writer.WriteNull("i64Field");
                    writer.WriteNull("strField");
                    writer.WriteNull("floatField");
                    writer.WriteNull("doubleField");
                    writer.WriteNull("boolField");
                    writer.WriteNull("guidField");
                    writer.WriteNull("dateField");
                    writer.WriteNull("timestampField");
                    writer.WriteNull("i8ArrayField");
                }
            }

            TestType Read(BinaryReader& reader)
            {
                int8_t i8Field = reader.ReadInt8("i8Field");
                int16_t i16Field = reader.ReadInt16("i16Field");
                int32_t i32Field = reader.ReadInt32("i32Field");
                int64_t i64Field = reader.ReadInt64("i64Field");
                std::string strField = reader.ReadString("strField");
                float floatField = reader.ReadFloat("floatField");
                double doubleField = reader.ReadDouble("doubleField");
                bool boolField = reader.ReadBool("boolField");
                Guid guidField = reader.ReadGuid("guidField");
                Date dateField = reader.ReadDate("dateField");
                Timestamp timestampField = reader.ReadTimestamp("timestampField");

                TestType result(i8Field, i16Field, i32Field, i64Field, strField,
                    floatField, doubleField, boolField, guidField, dateField,
                    timestampField);

                int32_t len = reader.ReadInt8Array("i8ArrayField", 0, 0);
                if (len > 0)
                {
                    result.i8ArrayField.resize(len);
                    reader.ReadInt8Array("i8ArrayField", &result.i8ArrayField[0], len);
                }
                return result;
            }

        IGNITE_BINARY_TYPE_END
    }
};

#endif // _IGNITE_ODBC_TEST_TEST_TYPE
