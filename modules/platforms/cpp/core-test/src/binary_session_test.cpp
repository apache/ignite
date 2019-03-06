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

#include <boost/test/unit_test.hpp>

#include "ignite/impl/interop/interop.h"
#include "ignite/impl/binary/binary_reader_impl.h"
#include "ignite/impl/binary/binary_writer_impl.h"

#include "ignite/binary_test_defs.h"

using namespace ignite;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;
using namespace ignite::binary;
using namespace ignite_test::core::binary;

/*
 * Check primitive value serialization-deserialization.
 */
template<typename T>
void CheckRawPrimitive(T writeVal) 
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem); 
    BinaryWriterImpl writeSes(&out, NULL);
    writeSes.WriteTopObject<T>(writeVal);
    out.Synchronize();

    InteropInputStream in(&mem); 
    BinaryReaderImpl reader(&in);
    T readVal = reader.ReadTopObject<T>();

    BOOST_REQUIRE(readVal == writeVal);
}

BOOST_AUTO_TEST_SUITE(BinarySessionTestSuite)

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
    BinaryWriterImpl writeSes(&out, NULL);
    writeSes.WriteTopObject<Guid>(writeVal);
    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    Guid readVal = reader.ReadTopObject<Guid>();

    BOOST_REQUIRE(readVal.GetMostSignificantBits() == writeVal.GetMostSignificantBits());
    BOOST_REQUIRE(readVal.GetLeastSignificantBits() == writeVal.GetLeastSignificantBits());    
}

BOOST_AUTO_TEST_CASE(TestDate)
{
    Date writeVal = Date(42);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writeSes(&out, NULL);
    writeSes.WriteTopObject<Date>(writeVal);
    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    Date readVal = reader.ReadTopObject<Date>();

    BOOST_REQUIRE(readVal == writeVal);
}

BOOST_AUTO_TEST_CASE(TestTime)
{
    Time writeVal = Time(42);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writeSes(&out, NULL);
    writeSes.WriteTopObject<Time>(writeVal);
    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    Time readVal = reader.ReadTopObject<Time>();

    BOOST_REQUIRE(readVal == writeVal);
}

BOOST_AUTO_TEST_CASE(TestTimestamp)
{
    Timestamp writeVal = Timestamp(77);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writeSes(&out, NULL);
    writeSes.WriteTopObject<Timestamp>(writeVal);
    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    Timestamp readVal = reader.ReadTopObject<Timestamp>();

    BOOST_CHECK(readVal == writeVal);
    BOOST_CHECK_EQUAL(readVal.GetMilliseconds(), writeVal.GetMilliseconds());
    BOOST_CHECK_EQUAL(readVal.GetSecondFraction(), writeVal.GetSecondFraction());
}

BOOST_AUTO_TEST_CASE(TestString)
{
    std::string writeVal = "MyString";

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writeSes(&out, NULL);
    writeSes.WriteTopObject<std::string>(writeVal);
    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    std::string readVal = reader.ReadTopObject<std::string>();

    BOOST_REQUIRE(readVal.compare(writeVal) == 0);
}

BOOST_AUTO_TEST_CASE(TestObject)
{
    InteropUnpooledMemory mem(1024);
    
    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);

    // 1. Test null object.
    BinaryInner writeVal(0);
    
    writer.WriteTopObject<BinaryInner>(writeVal);
    out.Synchronize();
    
    in.Synchronize();
    BinaryInner readVal = reader.ReadTopObject<BinaryInner>();

    BOOST_REQUIRE(readVal.GetValue() == 0);

    // 2. Test non-null object.
    out.Position(0);
    in.Position(0);

    writeVal = BinaryInner(1);

    writer.WriteTopObject<BinaryInner>(writeVal);
    out.Synchronize();

    in.Synchronize();
    readVal = reader.ReadTopObject<BinaryInner>();

    BOOST_REQUIRE(readVal.GetValue() == 1);
}

BOOST_AUTO_TEST_CASE(TestObjectWithRawFields)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);

    out.Position(0);
    in.Position(0);

    BinaryFields writeVal = BinaryFields(1, 2, 3, 4);

    writer.WriteTopObject<BinaryFields>(writeVal);
    out.Synchronize();

    in.Synchronize();
    BinaryFields readVal = reader.ReadTopObject<BinaryFields>();

    BOOST_REQUIRE(readVal.val1 == 1);
    BOOST_REQUIRE(readVal.val2 == 2);
    BOOST_REQUIRE(readVal.rawVal1 == 3);
    BOOST_REQUIRE(readVal.rawVal2 == 4);
}

BOOST_AUTO_TEST_CASE(TestPointer)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);

    // 1. Test null object.
    writer.WriteTopObject<BinaryInner*>(NULL);
    out.Synchronize();

    in.Synchronize();
    BinaryInner* readVal = reader.ReadTopObject<BinaryInner*>();

    BOOST_REQUIRE(readVal == NULL);

    // 2. Test non-null object.
    out.Position(0);
    in.Position(0);

    BinaryInner writeVal = BinaryInner(1);

    writer.WriteTopObject<BinaryInner*>(&writeVal);
    out.Synchronize();

    in.Synchronize();
    readVal = reader.ReadTopObject<BinaryInner*>();

    BOOST_REQUIRE(readVal->GetValue() == 1);

    delete readVal;
}

BOOST_AUTO_TEST_SUITE_END()
