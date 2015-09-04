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
    #define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "ignite/impl/interop/interop.h"
#include "ignite/impl/portable/portable_reader_impl.h"
#include "ignite/impl/portable/portable_writer_impl.h"

#include "ignite/portable_test_defs.h"

using namespace ignite;
using namespace ignite::impl::interop;
using namespace ignite::impl::portable;
using namespace ignite::portable;
using namespace ignite_test::core::portable;

/*
 * Check primitive value serialization-deserialization.
 */
template<typename T>
void CheckRawPrimitive(T writeVal) 
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem); 
    PortableWriterImpl writeSes(&out, NULL);
    writeSes.WriteTopObject<T>(writeVal);
    out.Synchronize();

    InteropInputStream in(&mem); 
    PortableReaderImpl reader(&in);
    T readVal = reader.ReadTopObject<T>();

    BOOST_REQUIRE(readVal == writeVal);
}

BOOST_AUTO_TEST_SUITE(PortableSessionTestSuite)

BOOST_AUTO_TEST_CASE(TestByte)
{
    CheckRawPrimitive<int8_t>(-128);
    CheckRawPrimitive<int8_t>(-1);
    CheckRawPrimitive<int8_t>(0);
    CheckRawPrimitive<int8_t>(1);
    CheckRawPrimitive<int8_t>(127);
}

BOOST_AUTO_TEST_CASE(TestBool)
{
    CheckRawPrimitive<bool>(true);
    CheckRawPrimitive<bool>(false);
}

BOOST_AUTO_TEST_CASE(TestShort)
{
    //CheckRawPrimitive<int16_t>(std::numeric_limits<int16_t>::min()); 
    CheckRawPrimitive<int16_t>(-1);
    CheckRawPrimitive<int16_t>(0);
    CheckRawPrimitive<int16_t>(1);
    //CheckRawPrimitive<int16_t>(std::numeric_limits<int16_t>::max());
}

BOOST_AUTO_TEST_CASE(TestChar)
{
    //CheckRawPrimitive<uint16_t>(std::numeric_limits<uint16_t>::min());
    CheckRawPrimitive<uint16_t>(1);
    //CheckRawPrimitive<uint16_t>(std::numeric_limits<uint16_t>::max());
}

BOOST_AUTO_TEST_CASE(TestInt)
{
    //CheckRawPrimitive<int32_t>(std::numeric_limits<int32_t>::min());
    CheckRawPrimitive<int32_t>(-1);
    CheckRawPrimitive<int32_t>(0);
    CheckRawPrimitive<int32_t>(1);
    //CheckRawPrimitive<int32_t>(std::numeric_limits<int32_t>::max());
}

BOOST_AUTO_TEST_CASE(TestLong)
{
    //CheckRawPrimitive<int64_t>(std::numeric_limits<int64_t>::min());
    CheckRawPrimitive<int64_t>(-1);
    CheckRawPrimitive<int64_t>(0);
    CheckRawPrimitive<int64_t>(1);
    //CheckRawPrimitive<int64_t>(std::numeric_limits<int64_t>::max());
}

BOOST_AUTO_TEST_CASE(TestFloat)
{
    CheckRawPrimitive<float>(-1.1f);
    CheckRawPrimitive<float>(0);
    CheckRawPrimitive<float>(1.1f);
}

BOOST_AUTO_TEST_CASE(TestDouble)
{
    CheckRawPrimitive<double>(-1.1);
    CheckRawPrimitive<double>(0);
    CheckRawPrimitive<double>(1.1);
}

BOOST_AUTO_TEST_CASE(TestGuid)
{
    Guid writeVal = Guid(1, 1);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writeSes(&out, NULL);
    writeSes.WriteTopObject<Guid>(writeVal);
    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    Guid readVal = reader.ReadTopObject<Guid>();

    BOOST_REQUIRE(readVal.GetMostSignificantBits() == writeVal.GetMostSignificantBits());
    BOOST_REQUIRE(readVal.GetLeastSignificantBits() == writeVal.GetLeastSignificantBits());    
}

BOOST_AUTO_TEST_CASE(TestString)
{
    std::string writeVal = "MyString";

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writeSes(&out, NULL);
    writeSes.WriteTopObject<std::string>(writeVal);
    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    std::string readVal = reader.ReadTopObject<std::string>();

    BOOST_REQUIRE(readVal.compare(writeVal) == 0);
}

BOOST_AUTO_TEST_CASE(TestObject)
{
    InteropUnpooledMemory mem(1024);
    
    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);

    // 1. Test null object.
    PortableInner writeVal(0);
    
    writer.WriteTopObject<PortableInner>(writeVal);
    out.Synchronize();
    
    in.Synchronize();
    PortableInner readVal = reader.ReadTopObject<PortableInner>();

    BOOST_REQUIRE(readVal.GetValue() == 0);

    // 2. Test non-null object.
    out.Position(0);
    in.Position(0);

    writeVal = PortableInner(1);

    writer.WriteTopObject<PortableInner>(writeVal);
    out.Synchronize();

    in.Synchronize();
    readVal = reader.ReadTopObject<PortableInner>();

    BOOST_REQUIRE(readVal.GetValue() == 1);
}

BOOST_AUTO_TEST_CASE(TestObjectWithRawFields)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);

    out.Position(0);
    in.Position(0);

    PortableFields writeVal = PortableFields(1, 2, 3, 4);

    writer.WriteTopObject<PortableFields>(writeVal);
    out.Synchronize();

    in.Synchronize();
    PortableFields readVal = reader.ReadTopObject<PortableFields>();

    BOOST_REQUIRE(readVal.val1 == 1);
    BOOST_REQUIRE(readVal.val2 == 2);
    BOOST_REQUIRE(readVal.rawVal1 == 3);
    BOOST_REQUIRE(readVal.rawVal2 == 4);
}

BOOST_AUTO_TEST_CASE(TestPointer)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);

    // 1. Test null object.
    writer.WriteTopObject<PortableInner*>(NULL);
    out.Synchronize();

    in.Synchronize();
    PortableInner* readVal = reader.ReadTopObject<PortableInner*>();

    BOOST_REQUIRE(readVal == NULL);

    // 2. Test non-null object.
    out.Position(0);
    in.Position(0);

    PortableInner writeVal = PortableInner(1);

    writer.WriteTopObject<PortableInner*>(&writeVal);
    out.Synchronize();

    in.Synchronize();
    readVal = reader.ReadTopObject<PortableInner*>();

    BOOST_REQUIRE(readVal->GetValue() == 1);

    delete readVal;
}

BOOST_AUTO_TEST_SUITE_END()