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
#include "ignite/portable/portable.h"

#include "ignite/portable_test_defs.h"
#include "ignite/portable_test_utils.h"

using namespace ignite;
using namespace ignite::impl::interop;
using namespace ignite::impl::portable;
using namespace ignite::portable;
using namespace ignite_test::core::portable;

template<typename T>
void CheckRawPrimitive(T val)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    Write<T>(rawWriter, val);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    T readVal = Read<T>(rawReader);
    
    BOOST_REQUIRE(readVal == val);
}

template<typename T>
void CheckRawPrimitiveArray(T dflt, T val1, T val2)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);
    
    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    // 1. Write NULL and see what happens.
    WriteArray<T>(rawWriter, NULL, 0);

    out.Synchronize();
    in.Synchronize();
    
    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 0) == -1);

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 2) == -1);

    T arr1[2];
    arr1[0] = dflt;
    arr1[1] = dflt;

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 1) == -1);

    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    // 2. Write empty array.
    T arr2[2];
    arr2[0] = val1;
    arr2[1] = val2;

    out.Position(0);
    in.Position(0);

    WriteArray<T>(rawWriter, arr2, 0);

    out.Synchronize();
    in.Synchronize();

    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 0) == 0);

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 2) == 0);

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 0) == 0);
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 2) == 0);
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    // 3. Partial array write.
    out.Position(0);
    in.Position(0);

    WriteArray<T>(rawWriter, arr2, 1);

    out.Synchronize();
    in.Synchronize();

    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 0) == 1);
    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 2) == 1);

    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 0) == 1);
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 1) == 1);
    BOOST_REQUIRE(arr1[0] == val1);
    BOOST_REQUIRE(arr1[1] == dflt);
    arr1[0] = dflt;

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 2) == 1);
    BOOST_REQUIRE(arr1[0] == val1);
    BOOST_REQUIRE(arr1[1] == dflt);
    arr1[0] = dflt;

    // 4. Full array write.
    out.Position(0);
    in.Position(0);

    WriteArray<T>(rawWriter, arr2, 2);

    out.Synchronize();
    in.Synchronize();

    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 0) == 2);
    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 2) == 2);

    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 0) == 2);
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 1) == 2);
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 2) == 2);
    BOOST_REQUIRE(arr1[0] == val1);
    BOOST_REQUIRE(arr1[1] == val2);
}

void CheckRawWritesRestricted(PortableRawWriter& writer)
{
    try
    {
        writer.WriteInt8(1);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        int8_t arr[1];

        writer.WriteInt8Array(arr, 1);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        Guid val(1, 1);

        writer.WriteGuid(val);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        writer.WriteString("test");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try 
    {
        writer.WriteArray<int8_t>();

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try 
    {
        writer.WriteCollection<int8_t>();

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try 
    {
        writer.WriteMap<int8_t, int8_t>();

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }
}

void CheckRawReadsRestricted(PortableRawReader& reader)
{
    try
    {
        reader.ReadInt8();

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        int8_t arr[1];

        reader.ReadInt8Array(arr, 1);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadGuid();

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadString();

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadArray<int8_t>();

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadCollection<int8_t>();

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadMap<int8_t, int8_t>();

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }
}

void CheckRawCollectionEmpty(CollectionType* colType)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    PortableCollectionWriter<PortableInner> colWriter = colType ?
        rawWriter.WriteCollection<PortableInner>(*colType) : rawWriter.WriteCollection<PortableInner>();

    CheckRawWritesRestricted(rawWriter);

    colWriter.Close();

    rawWriter.WriteInt8(1);

    try
    {
        colWriter.Write(1);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        colWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableCollectionReader<PortableInner> colReader = rawReader.ReadCollection<PortableInner>();

    if (colType)
        BOOST_REQUIRE(colReader.GetType() == *colType);
    else
        BOOST_REQUIRE(colReader.GetType() == IGNITE_COLLECTION_UNDEFINED);

    BOOST_REQUIRE(colReader.GetSize() == 0);
    BOOST_REQUIRE(!colReader.HasNext());
    BOOST_REQUIRE(!colReader.IsNull());

    try
    {
        colReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

void CheckRawCollection(CollectionType* colType)
{
    PortableInner writeVal1 = PortableInner(1);
    PortableInner writeVal2 = PortableInner(0);
    PortableInner writeVal3 = PortableInner(2);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    PortableCollectionWriter<PortableInner> colWriter = colType ?
        rawWriter.WriteCollection<PortableInner>(*colType) : rawWriter.WriteCollection<PortableInner>();

    colWriter.Write(writeVal1);
    colWriter.Write(writeVal2);
    colWriter.Write(writeVal3);

    CheckRawWritesRestricted(rawWriter);

    colWriter.Close();

    rawWriter.WriteInt8(1);

    try
    {
        colWriter.Write(1);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        colWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableCollectionReader<PortableInner> colReader = rawReader.ReadCollection<PortableInner>();

    CheckRawReadsRestricted(rawReader);

    if (colType)
        BOOST_REQUIRE(colReader.GetType() == *colType);
    else
        BOOST_REQUIRE(colReader.GetType() == IGNITE_COLLECTION_UNDEFINED);

    BOOST_REQUIRE(colReader.GetSize() == 3);
    BOOST_REQUIRE(!colReader.IsNull());

    BOOST_REQUIRE(colReader.HasNext());
    BOOST_REQUIRE(colReader.GetNext().GetValue() == writeVal1.GetValue());

    BOOST_REQUIRE(colReader.HasNext());
    BOOST_REQUIRE(colReader.GetNext().GetValue() == writeVal2.GetValue());

    BOOST_REQUIRE(colReader.HasNext());
    BOOST_REQUIRE(colReader.GetNext().GetValue() == writeVal3.GetValue());

    BOOST_REQUIRE(!colReader.HasNext());

    try
    {
        colReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

void CheckRawMapEmpty(MapType* mapType)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    PortableMapWriter<int8_t, PortableInner> mapWriter = mapType ?
        rawWriter.WriteMap<int8_t, PortableInner>(*mapType) : rawWriter.WriteMap<int8_t, PortableInner>();

    CheckRawWritesRestricted(rawWriter);

    mapWriter.Close();

    rawWriter.WriteInt8(1);

    try
    {
        mapWriter.Write(1, PortableInner(1));

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        mapWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableMapReader<int8_t, PortableInner> mapReader = rawReader.ReadMap<int8_t, PortableInner>();

    if (mapType)
        BOOST_REQUIRE(mapReader.GetType() == *mapType);
    else
        BOOST_REQUIRE(mapReader.GetType() == IGNITE_MAP_UNDEFINED);

    BOOST_REQUIRE(mapReader.GetSize() == 0);
    BOOST_REQUIRE(!mapReader.HasNext());
    BOOST_REQUIRE(!mapReader.IsNull());

    try
    {
        int8_t key;
        PortableInner val;

        mapReader.GetNext(&key, &val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

void CheckRawMap(MapType* mapType)
{
    PortableInner writeVal1 = PortableInner(1);
    PortableInner writeVal2 = PortableInner(0);
    PortableInner writeVal3 = PortableInner(2);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    PortableMapWriter<int8_t, PortableInner> mapWriter = mapType ?
        rawWriter.WriteMap<int8_t, PortableInner>(*mapType) : rawWriter.WriteMap<int8_t, PortableInner>();

    mapWriter.Write(1, writeVal1);
    mapWriter.Write(2, writeVal2);
    mapWriter.Write(3, writeVal3);

    CheckRawWritesRestricted(rawWriter);

    mapWriter.Close();

    rawWriter.WriteInt8(1);

    try
    {
        mapWriter.Write(4, PortableInner(4));

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        mapWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableMapReader<int8_t, PortableInner> mapReader = rawReader.ReadMap<int8_t, PortableInner>();

    CheckRawReadsRestricted(rawReader);

    if (mapType)
        BOOST_REQUIRE(mapReader.GetType() == *mapType);
    else
        BOOST_REQUIRE(mapReader.GetType() == IGNITE_MAP_UNDEFINED);

    BOOST_REQUIRE(mapReader.GetSize() == 3);
    BOOST_REQUIRE(!mapReader.IsNull());

    int8_t key;
    PortableInner val;

    BOOST_REQUIRE(mapReader.HasNext());

    mapReader.GetNext(&key, &val);
    BOOST_REQUIRE(key == 1);
    BOOST_REQUIRE(val.GetValue() == writeVal1.GetValue());

    mapReader.GetNext(&key, &val);
    BOOST_REQUIRE(key == 2);
    BOOST_REQUIRE(val.GetValue() == writeVal2.GetValue());

    mapReader.GetNext(&key, &val);
    BOOST_REQUIRE(key == 3);
    BOOST_REQUIRE(val.GetValue() == writeVal3.GetValue());

    BOOST_REQUIRE(!mapReader.HasNext());

    try
    {
        mapReader.GetNext(&key, &val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_SUITE(PortableReaderWriterRawTestSuite)

BOOST_AUTO_TEST_CASE(TestPrimitiveInt8)
{
    CheckRawPrimitive<int8_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveBool)
{
    CheckRawPrimitive<bool>(true);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveInt16)
{
    CheckRawPrimitive<int16_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveUInt16)
{
    CheckRawPrimitive<uint16_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveInt32)
{
    CheckRawPrimitive<int32_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveInt64)
{
    CheckRawPrimitive<int64_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveFloat)
{
    CheckRawPrimitive<float>(1.1f);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveDouble)
{
    CheckRawPrimitive<double>(1.1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveGuid)
{
    Guid val(1, 2);

    CheckRawPrimitive<Guid>(val);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayInt8)
{
    CheckRawPrimitiveArray<int8_t>(1, 2, 3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayBool)
{
    CheckRawPrimitiveArray<bool>(false, true, false);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayInt16)
{
    CheckRawPrimitiveArray<int16_t>(1, 2, 3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayUInt16)
{
    CheckRawPrimitiveArray<uint16_t>(1, 2, 3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayInt32)
{
    CheckRawPrimitiveArray<int32_t>(1, 2, 3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayInt64)
{
    CheckRawPrimitiveArray<int64_t>(1, 2, 3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayFloat)
{
    CheckRawPrimitiveArray<float>(1.1f, 2.2f, 3.3f);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayDouble)
{
    CheckRawPrimitiveArray<double>(1.1, 2.2, 3.3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayGuid)
{
    Guid dflt(1, 2);
    Guid val1(3, 4);
    Guid val2(5, 6);

    CheckRawPrimitiveArray<Guid>(dflt, val1, val2);
}

BOOST_AUTO_TEST_CASE(TestGuidNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    rawWriter.WriteNull();

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    Guid expVal;
    Guid actualVal = rawReader.ReadGuid();

    BOOST_REQUIRE(actualVal == expVal);
}

BOOST_AUTO_TEST_CASE(TestString) {
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    const char* writeVal1 = "testtest";
    const char* writeVal2 = "test";
    std::string writeVal3 = writeVal1;

    rawWriter.WriteString(writeVal1);
    rawWriter.WriteString(writeVal1, 4);
    rawWriter.WriteString(writeVal3);
    rawWriter.WriteString(NULL);
    rawWriter.WriteString(NULL, 4);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    char readVal1[9];
    char readVal2[5];
    
    BOOST_REQUIRE(rawReader.ReadString(NULL, 0) == 8);
    BOOST_REQUIRE(rawReader.ReadString(NULL, 8) == 8);
    BOOST_REQUIRE(rawReader.ReadString(readVal1, 0) == 8);
    BOOST_REQUIRE(rawReader.ReadString(readVal1, 4) == 8);

    BOOST_REQUIRE(rawReader.ReadString(readVal1, 9) == 8);
    std::string writeVal1Str = writeVal1;
    std::string readVal1Str = readVal1;
    BOOST_REQUIRE(readVal1Str.compare(writeVal1Str) == 0);

    BOOST_REQUIRE(rawReader.ReadString(readVal2, 5) == 4);
    std::string writeVal2Str = writeVal2;
    std::string readVal2Str = readVal2;
    BOOST_REQUIRE(readVal2Str.compare(writeVal2Str) == 0);

    std::string readVal3 = rawReader.ReadString();
    BOOST_REQUIRE(readVal3.compare(writeVal3) == 0);

    BOOST_REQUIRE(rawReader.ReadString(readVal1, 9) == -1);
    BOOST_REQUIRE(rawReader.ReadString(readVal1, 9) == -1);
}

BOOST_AUTO_TEST_CASE(TestStringArrayNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    rawWriter.WriteNull();
    rawWriter.WriteInt8(1);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableStringArrayReader arrReader = rawReader.ReadStringArray();

    BOOST_REQUIRE(arrReader.GetSize() == -1);
    BOOST_REQUIRE(!arrReader.HasNext());
    BOOST_REQUIRE(arrReader.IsNull());

    try
    {
        char res[100];

        arrReader.GetNext(res, 100);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        arrReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestStringArrayEmpty)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    PortableStringArrayWriter arrWriter = rawWriter.WriteStringArray();

    CheckRawWritesRestricted(rawWriter);

    arrWriter.Close();

    rawWriter.WriteInt8(1);

    try
    {
        const char* val = "test";

        arrWriter.Write(val, 4);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        const char* val = "test";

        arrWriter.Write(val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        std::string val = "test";

        arrWriter.Write(val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        arrWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableStringArrayReader arrReader = rawReader.ReadStringArray();

    BOOST_REQUIRE(arrReader.GetSize() == 0);
    BOOST_REQUIRE(!arrReader.HasNext());
    BOOST_REQUIRE(!arrReader.IsNull());

    try
    {
        char res[100];

        arrReader.GetNext(res, 100);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        arrReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestStringArray)
{
    const char* writeVal1 = "testtest";
    const char* writeVal2 = "test";
    std::string writeVal3 = "test2";

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    PortableStringArrayWriter arrWriter = rawWriter.WriteStringArray();

    arrWriter.Write(writeVal1);
    arrWriter.Write(writeVal1, 4);
    arrWriter.Write(NULL); // NULL value.
    arrWriter.Write(NULL, 100); // NULL value again.
    arrWriter.Write(writeVal3);

    CheckRawWritesRestricted(rawWriter);

    arrWriter.Close();

    rawWriter.WriteInt8(1);

    try
    {
        const char* val = "test";

        arrWriter.Write(val, 4);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        const char* val = "test";

        arrWriter.Write(val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        std::string val = "test";

        arrWriter.Write(val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        arrWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableStringArrayReader arrReader = rawReader.ReadStringArray();

    CheckRawReadsRestricted(rawReader);

    BOOST_REQUIRE(arrReader.GetSize() == 5);
    BOOST_REQUIRE(!arrReader.IsNull());

    // 1. Read first value.
    BOOST_REQUIRE(arrReader.HasNext());
        
    char readVal1[9];
    
    BOOST_REQUIRE(arrReader.GetNext(NULL, 0) == 8);
    BOOST_REQUIRE(arrReader.GetNext(NULL, 8) == 8);
    BOOST_REQUIRE(arrReader.GetNext(readVal1, 0) == 8);
    BOOST_REQUIRE(arrReader.GetNext(readVal1, 4) == 8);

    BOOST_REQUIRE(arrReader.GetNext(readVal1, 9) == 8);
    std::string writeVal1Str = writeVal1;
    std::string readVal1Str = readVal1;
    BOOST_REQUIRE(readVal1Str.compare(writeVal1Str) == 0);

    // 2. Read second value.
    BOOST_REQUIRE(arrReader.HasNext());

    char readVal2[5];

    BOOST_REQUIRE(arrReader.GetNext(readVal2, 5) == 4);
    std::string writeVal2Str = writeVal2;
    std::string readVal2Str = readVal2;
    BOOST_REQUIRE(readVal2Str.compare(writeVal2Str) == 0);

    // 3. Read NULL.
    BOOST_REQUIRE(arrReader.HasNext());

    BOOST_REQUIRE(arrReader.GetNext(readVal1, 4) == -1);
    readVal1Str = readVal1;
    BOOST_REQUIRE(readVal1Str.compare(writeVal1Str) == 0);

    // 4. Read NULL again, this time through another method.
    BOOST_REQUIRE(arrReader.HasNext());

    std::string readNullVal = arrReader.GetNext();

    BOOST_REQUIRE(readNullVal.length() == 0);

    // 5. Read third value.
    BOOST_REQUIRE(arrReader.HasNext());

    std::string readVal3 = arrReader.GetNext();
    BOOST_REQUIRE(readVal3.compare(writeVal3) == 0);

    BOOST_REQUIRE(!arrReader.HasNext());

    try
    {
        char res[100];

        arrReader.GetNext(res, 100);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        arrReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestObject)
{
    PortableInner writeVal1(1);
    PortableInner writeVal2(0);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    rawWriter.WriteObject(writeVal1);
    rawWriter.WriteObject(writeVal2);
    rawWriter.WriteNull();

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableInner readVal1 = rawReader.ReadObject<PortableInner>();
    BOOST_REQUIRE(writeVal1.GetValue() == readVal1.GetValue());

    PortableInner readVal2 = rawReader.ReadObject<PortableInner>();
    BOOST_REQUIRE(writeVal2.GetValue() == readVal2.GetValue());

    PortableInner readVal3 = rawReader.ReadObject<PortableInner>();
    BOOST_REQUIRE(0 == readVal3.GetValue());
}

BOOST_AUTO_TEST_CASE(TestNestedObject)
{
    PortableOuter writeVal1(1, 2);
    PortableOuter writeVal2(0, 0);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    rawWriter.WriteObject(writeVal1);
    rawWriter.WriteObject(writeVal2);
    rawWriter.WriteNull();

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableOuter readVal1 = rawReader.ReadObject<PortableOuter>();
    BOOST_REQUIRE(writeVal1.GetValue() == readVal1.GetValue());
    BOOST_REQUIRE(writeVal1.GetInner().GetValue() == readVal1.GetInner().GetValue());

    PortableOuter readVal2 = rawReader.ReadObject<PortableOuter>();
    BOOST_REQUIRE(writeVal2.GetValue() == readVal2.GetValue());
    BOOST_REQUIRE(writeVal2.GetInner().GetValue() == readVal2.GetInner().GetValue());

    PortableOuter readVal3 = rawReader.ReadObject<PortableOuter>();
    BOOST_REQUIRE(0 == readVal3.GetValue());
    BOOST_REQUIRE(0 == readVal3.GetInner().GetValue());
}

BOOST_AUTO_TEST_CASE(TestArrayNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    rawWriter.WriteNull();
    rawWriter.WriteInt8(1);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableArrayReader<PortableInner> arrReader = rawReader.ReadArray<PortableInner>();

    BOOST_REQUIRE(arrReader.GetSize() == -1);
    BOOST_REQUIRE(!arrReader.HasNext());
    BOOST_REQUIRE(arrReader.IsNull());

    try
    {
        arrReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestArrayEmpty) 
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    PortableArrayWriter<PortableInner> arrWriter = rawWriter.WriteArray<PortableInner>();

    CheckRawWritesRestricted(rawWriter);

    arrWriter.Close();

    rawWriter.WriteInt8(1);

    try
    {
        arrWriter.Write(1);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        arrWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableArrayReader<PortableInner> arrReader = rawReader.ReadArray<PortableInner>();

    BOOST_REQUIRE(arrReader.GetSize() == 0);
    BOOST_REQUIRE(!arrReader.HasNext());
    BOOST_REQUIRE(!arrReader.IsNull());

    try
    {
        arrReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestArray)
{
    PortableInner writeVal1 = PortableInner(1);
    PortableInner writeVal2 = PortableInner(0);
    PortableInner writeVal3 = PortableInner(2);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    PortableArrayWriter<PortableInner> arrWriter = rawWriter.WriteArray<PortableInner>();

    arrWriter.Write(writeVal1); 
    arrWriter.Write(writeVal2);
    arrWriter.Write(writeVal3);

    CheckRawWritesRestricted(rawWriter);

    arrWriter.Close();

    rawWriter.WriteInt8(1);

    try
    {
        arrWriter.Write(1);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        arrWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableArrayReader<PortableInner> arrReader = rawReader.ReadArray<PortableInner>();

    CheckRawReadsRestricted(rawReader);

    BOOST_REQUIRE(arrReader.GetSize() == 3);
    BOOST_REQUIRE(!arrReader.IsNull());

    BOOST_REQUIRE(arrReader.HasNext());
    BOOST_REQUIRE(arrReader.GetNext().GetValue() == writeVal1.GetValue());

    BOOST_REQUIRE(arrReader.HasNext());
    BOOST_REQUIRE(arrReader.GetNext().GetValue() == writeVal2.GetValue());

    BOOST_REQUIRE(arrReader.HasNext());
    BOOST_REQUIRE(arrReader.GetNext().GetValue() == writeVal3.GetValue());

    BOOST_REQUIRE(!arrReader.HasNext());

    try
    {
        arrReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestCollectionNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    rawWriter.WriteNull();
    rawWriter.WriteInt8(1);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableCollectionReader<PortableInner> colReader = rawReader.ReadCollection<PortableInner>();

    BOOST_REQUIRE(colReader.GetType() == IGNITE_COLLECTION_UNDEFINED);
    BOOST_REQUIRE(colReader.GetSize() == -1);
    BOOST_REQUIRE(!colReader.HasNext());
    BOOST_REQUIRE(colReader.IsNull()); 

    try
    {
        colReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestCollectionEmpty)
{
    CheckRawCollectionEmpty(NULL);
}

BOOST_AUTO_TEST_CASE(TestCollectionEmptyTyped)
{
    CollectionType typ = IGNITE_COLLECTION_CONCURRENT_SKIP_LIST_SET;

    CheckRawCollectionEmpty(&typ);
}

BOOST_AUTO_TEST_CASE(TestCollection)
{
    CheckRawCollection(NULL);
}

BOOST_AUTO_TEST_CASE(testCollectionTyped)
{
    CollectionType typ = IGNITE_COLLECTION_CONCURRENT_SKIP_LIST_SET;

    CheckRawCollection(&typ);
}

BOOST_AUTO_TEST_CASE(TestMapNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);
    PortableRawWriter rawWriter(&writer);

    rawWriter.WriteNull();
    rawWriter.WriteInt8(1);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl reader(&in);
    PortableRawReader rawReader(&reader);

    PortableMapReader<int8_t, PortableInner> mapReader = rawReader.ReadMap<int8_t, PortableInner>();

    BOOST_REQUIRE(mapReader.GetType() == IGNITE_MAP_UNDEFINED);
    BOOST_REQUIRE(mapReader.GetSize() == -1);
    BOOST_REQUIRE(!mapReader.HasNext());
    BOOST_REQUIRE(mapReader.IsNull());

    try
    {
        int8_t key;
        PortableInner val;

        mapReader.GetNext(&key, &val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestMapEmpty)
{
    CheckRawMapEmpty(NULL);
}

BOOST_AUTO_TEST_CASE(TestMapEmptyTyped)
{
    MapType typ = IGNITE_MAP_CONCURRENT_HASH_MAP;

    CheckRawMapEmpty(&typ);
}

BOOST_AUTO_TEST_CASE(TestMap)
{
    CheckRawMap(NULL);
}

BOOST_AUTO_TEST_CASE(TestMapTyped)
{
    MapType typ = IGNITE_MAP_CONCURRENT_HASH_MAP;

    CheckRawMap(&typ);
}

BOOST_AUTO_TEST_SUITE_END()