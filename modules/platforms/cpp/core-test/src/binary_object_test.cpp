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

    BinaryObject obj(BinaryObjectImpl::FromMemory(mem, 0));

    T actual = obj.Deserialize<T>();

    BOOST_REQUIRE_EQUAL(value, actual);
}

template<typename T>
void CheckSimpleNP(const T& value)
{
    InteropUnpooledMemory mem(1024);

    FillMem<T>(mem, value);

    BinaryObject obj(BinaryObjectImpl::FromMemory(mem, 0));

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

    BinaryObjectImpl binObj(BinaryObjectImpl::FromMemory(mem, 0));

    BOOST_REQUIRE_EQUAL(binObj.GetLength(), objData.GetSize());

    common::FixedSizeArray<int8_t> binObjData(binObj.GetData(), binObj.GetLength());

    for (int32_t i = 0; i < objData.GetSize(); ++i)
        BOOST_CHECK_EQUAL(objData[i], binObjData[i]);
}

BOOST_AUTO_TEST_SUITE(BinaryObjectTestSuite)

BOOST_AUTO_TEST_CASE(UserTestType)
{
    CheckSimpleNP(TestType());
    CheckSimpleNP(TestType(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9),
        common::MakeDateGmt(1987, 6, 5),
        common::MakeTimeGmt(13, 32, 9),
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
        common::MakeTimeGmt(13, 32, 9),
        common::MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456)));
}

BOOST_AUTO_TEST_CASE(UserBinaryFieldsGetData)
{
    CheckData(BinaryFields());
    CheckData(BinaryFields(423425, 961851, 18946, 180269165));
}

BOOST_AUTO_TEST_SUITE_END()