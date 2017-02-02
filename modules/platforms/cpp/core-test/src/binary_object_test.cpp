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

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include <ignite/common/fixed_size_array.h>
#include <ignite/binary/binary_object.h>
#include <ignite/binary/binary_writer.h>

#include "ignite/binary_test_defs.h"
#include "ignite/test_type.h"
#include "ignite/complex_type.h"

using namespace ignite;
using namespace ignite::binary;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;
using namespace ignite_test::core::binary;

template<typename T>
void FillMem(InteropMemory& mem, const T& value)
{
    InteropOutputStream stream(&mem);
    BinaryWriterImpl writer(&stream, 0);

    writer.WriteObject<T>(value);

    stream.Synchronize();
}

template<typename T>
void CheckSimple(const T& value)
{
    InteropUnpooledMemory mem(1024);

    FillMem<T>(mem, value);

    BinaryObject obj(mem, 0);

    T actual = obj.Deserialize<T>();

    BOOST_REQUIRE_EQUAL(value, actual);
}

template<typename T>
void CheckSimpleNP(const T& value)
{
    InteropUnpooledMemory mem(1024);

    FillMem<T>(mem, value);

    BinaryObject obj(mem, 0);

    T actual = obj.Deserialize<T>();

    BOOST_REQUIRE(value == actual);
}

template<typename T>
void GetObjectData(const T& obj, common::FixedSizeArray<int8_t>& data)
{
    DummyIdResolver idResolver;

    InteropUnpooledMemory mem(1024);
    InteropOutputStream stream(&mem);
    BinaryWriterImpl writerImpl(&stream, &idResolver, 0, 0, 0);
    BinaryWriter writer(&writerImpl);

    BinaryType<T> bt;

    bt.Write(writer, obj);

    data.Assign(mem.Data(), stream.Position());
}

template<typename T>
void CheckData(const T& obj)
{
    common::FixedSizeArray<int8_t> objData;
    GetObjectData<T>(obj, objData);

    InteropUnpooledMemory mem(1024);
    FillMem<T>(mem, obj);

    BinaryObjectImpl binObj(mem, 0);

    BOOST_REQUIRE_EQUAL(binObj.GetLength(), objData.GetSize());

    common::FixedSizeArray<int8_t> binObjData(binObj.GetData(), binObj.GetLength());

    for (int32_t i = 0; i < objData.GetSize(); ++i)
        BOOST_CHECK_EQUAL(objData[i], binObjData[i]);
}

BOOST_AUTO_TEST_SUITE(BinaryObjectTestSuite)

#ifdef CHECK_BINARY_OBJECT_WITH_PRIMITIVES

BOOST_AUTO_TEST_CASE(PrimitiveInt8)
{
    CheckSimple<int8_t>(0);
    CheckSimple<int8_t>(INT8_MAX);
    CheckSimple<int8_t>(INT8_MIN);
    CheckSimple<int8_t>(42);
    CheckSimple<int8_t>(-12);
    CheckSimple<int8_t>(0x7D);
}

BOOST_AUTO_TEST_CASE(PrimitiveInt16)
{
    CheckSimple<int32_t>(0);
    CheckSimple<int32_t>(INT16_MAX);
    CheckSimple<int32_t>(INT16_MIN);
    CheckSimple<int32_t>(42);
    CheckSimple<int32_t>(12321);
    CheckSimple<int32_t>(0x7AB0);
}

BOOST_AUTO_TEST_CASE(PrimitiveInt32)
{
    CheckSimple<int32_t>(0);
    CheckSimple<int32_t>(INT32_MAX);
    CheckSimple<int32_t>(INT32_MIN);
    CheckSimple<int32_t>(42);
    CheckSimple<int32_t>(1337);
    CheckSimple<int32_t>(0xA2496BC9);
}

BOOST_AUTO_TEST_CASE(PrimitiveInt64)
{
    CheckSimple<int64_t>(0);
    CheckSimple<int64_t>(INT64_MAX);
    CheckSimple<int64_t>(INT64_MIN);
    CheckSimple<int64_t>(42);
    CheckSimple<int64_t>(13371337133713371337LL);
    CheckSimple<int64_t>(0xA928673F501CC09E);
}

BOOST_AUTO_TEST_CASE(PrimitiveBool)
{
    CheckSimple<bool>(true);
    CheckSimple<bool>(false);
}

BOOST_AUTO_TEST_CASE(PrimitiveFloat)
{
    CheckSimple<float>(0.0);
    CheckSimple<float>(1E38f);
    CheckSimple<float>(-1E38f);
    CheckSimple<float>(1E-38f);
    CheckSimple<float>(-1E-38f);
    CheckSimple<float>(42.0f);
    CheckSimple<float>(42.42f);
    CheckSimple<float>(1337.1337f);
}

BOOST_AUTO_TEST_CASE(PrimitiveDouble)
{
    CheckSimple<double>(0);
    CheckSimple<double>(1E127);
    CheckSimple<double>(-1E127);
    CheckSimple<double>(1E-127);
    CheckSimple<double>(-1E-127);
    CheckSimple<double>(42);
    CheckSimple<double>(42.42);
    CheckSimple<double>(1337.1337 * 1337.1337);
}

BOOST_AUTO_TEST_CASE(PrimitiveString)
{
    CheckSimple<std::string>("");
    CheckSimple<std::string>("Lorem ipsum");
    CheckSimple<std::string>("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
        "eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, "
        "quis nostrud exercitation");

    CheckSimple<std::string>(std::string(1000, '.'));
}

BOOST_AUTO_TEST_CASE(PrimitiveGuid)
{
    CheckSimple<Guid>(Guid(0, 0));
    CheckSimple<Guid>(Guid(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF));
    CheckSimple<Guid>(Guid(0x4F9039DEF0FB8000, 0x905AE8A2D6FD49C1));
}

BOOST_AUTO_TEST_CASE(PrimitiveDate)
{
    CheckSimpleNP<Date>(Date(0));
    CheckSimpleNP<Date>(common::MakeDateGmt(1998, 12, 3, 18, 32, 01));
    CheckSimpleNP<Date>(common::MakeDateGmt(2017, 1, 18, 20, 50, 41));
    CheckSimpleNP<Date>(common::MakeDateLocal(1998, 12, 3, 18, 32, 01));
}

BOOST_AUTO_TEST_CASE(PrimitiveTimestamp)
{
    CheckSimpleNP<Timestamp>(Timestamp(0));
    CheckSimpleNP<Timestamp>(common::MakeTimestampGmt(1998, 12, 3, 18, 32, 01, 593846589));
    CheckSimpleNP<Timestamp>(common::MakeTimestampGmt(2017, 1, 18, 20, 50, 41, 920700532));
    CheckSimpleNP<Timestamp>(common::MakeTimestampLocal(1998, 12, 3, 18, 32, 01, 2385));
}

#endif //CHECK_BINARY_OBJECT_WITH_PRIMITIVES

BOOST_AUTO_TEST_CASE(UserTestType)
{
    CheckSimpleNP(TestType());
    CheckSimpleNP(TestType(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9),
        common::MakeDateGmt(1987, 6, 5),
        common::MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456)));
}

BOOST_AUTO_TEST_CASE(UserComplexType)
{
    CheckSimpleNP(ComplexType());

    ComplexType nonDefault;

    nonDefault.i32Field = 589630659;
    nonDefault.strField = "Some string value";
    nonDefault.objField.f1 = 403685016;
    nonDefault.objField.f2 = "Whatever";

    CheckSimpleNP(nonDefault);
}

BOOST_AUTO_TEST_CASE(UserBinaryFields)
{
    CheckSimpleNP(BinaryFields());

    BinaryFields nonDefault(423425, 961851, 18946, 180269165);

    CheckSimpleNP(nonDefault);
}

BOOST_AUTO_TEST_CASE(UserComplexTypeGetData)
{
    CheckData(ComplexType());

    ComplexType nonDefault;

    nonDefault.i32Field = 589630659;
    nonDefault.strField = "Some string value";
    nonDefault.objField.f1 = 403685016;
    nonDefault.objField.f2 = "Whatever";

    CheckData(nonDefault);
}

BOOST_AUTO_TEST_CASE(UserTestTypeGetData)
{
    CheckData(TestType());
    CheckData(TestType(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9),
        common::MakeDateGmt(1987, 6, 5),
        common::MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456)));
}

BOOST_AUTO_TEST_CASE(UserBinaryFieldsGetData)
{
    CheckData(BinaryFields());
    CheckData(BinaryFields(423425, 961851, 18946, 180269165));
}

BOOST_AUTO_TEST_SUITE_END()