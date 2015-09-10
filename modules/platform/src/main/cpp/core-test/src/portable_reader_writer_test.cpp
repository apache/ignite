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
void CheckPrimitive(T val)
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    out.Position(18);

    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    try
    {
        Write<T>(writer, NULL, val);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    Write<T>(writer, "test", val);

    out.Synchronize();

    InteropInputStream in(&mem);

    in.Position(18);

    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100);
    PortableReader reader(&readerImpl);

    try
    {
        Read<T>(reader, NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    T readVal = Read<T>(reader, "test");
    
    BOOST_REQUIRE(readVal == val);
}

template<typename T>
void CheckPrimitiveArray(T dflt, T val1, T val2)
{
    const char* fieldName = "test";

    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);
    
    InteropInputStream in(&mem);
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100);
    PortableReader reader(&readerImpl);

    out.Position(18);

    try
    {
        T nullFieldArr[2];

        nullFieldArr[0] = val1;
        nullFieldArr[1] = val2;

        WriteArray<T>(writer, NULL, nullFieldArr, 2);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }
    
    // 1. Write NULL and see what happens.
    WriteArray<T>(writer, fieldName, NULL, 0);

    out.Synchronize();
    in.Synchronize();
    
    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 0) == -1);

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 2) == -1);

    T arr1[2];
    arr1[0] = dflt;
    arr1[1] = dflt;

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 1) == -1);

    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    // 2. Write empty array.
    T arr2[2];
    arr2[0] = val1;
    arr2[1] = val2;

    out.Position(18);
    
    WriteArray<T>(writer, fieldName, arr2, 0);

    out.Synchronize();
    in.Synchronize();

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 0) == 0);

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 2) == 0);

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 0) == 0);
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 2) == 0);
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    // 3. Partial array write.
    out.Position(18);
    
    WriteArray<T>(writer, fieldName, arr2, 1);

    out.Synchronize();
    in.Synchronize();

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 0) == 1);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 2) == 1);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 0) == 1);
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 1) == 1);
    BOOST_REQUIRE(arr1[0] == val1);
    BOOST_REQUIRE(arr1[1] == dflt);
    arr1[0] = dflt;

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 2) == 1);
    BOOST_REQUIRE(arr1[0] == val1);
    BOOST_REQUIRE(arr1[1] == dflt);
    arr1[0] = dflt;

    // 4. Full array write.
    out.Position(18);
    
    WriteArray<T>(writer, fieldName, arr2, 2);

    out.Synchronize();
    in.Synchronize();

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 0) == 2);

    in.Position(18);
    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 2) == 2);

    try
    {
        ReadArray<T>(reader, NULL, arr1, 2);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 0) == 2);
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 1) == 2);
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 2) == 2);
    BOOST_REQUIRE(arr1[0] == val1);
    BOOST_REQUIRE(arr1[1] == val2);
}

void CheckWritesRestricted(PortableWriter& writer)
{
    try
    {
        writer.WriteInt8("field", 1);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        int8_t arr[1];

        writer.WriteInt8Array("field", arr, 1);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        Guid val(1, 1);

        writer.WriteGuid("field", val);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        writer.WriteString("field", "test");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try 
    {
        writer.WriteArray<int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try 
    {
        writer.WriteCollection<int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try 
    {
        writer.WriteMap<int8_t, int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }
}

void CheckReadsRestricted(PortableReader& reader)
{
    try
    {
        reader.ReadInt8("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        int8_t arr[1];

        reader.ReadInt8Array("field", arr, 1);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadGuid("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadString("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadArray<int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadCollection<int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadMap<int8_t, int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }
}

void CheckCollectionEmpty(CollectionType* colType)
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    PortableCollectionWriter<PortableInner> colWriter = colType ?
        writer.WriteCollection<PortableInner>("field1", *colType) : writer.WriteCollection<PortableInner>("field1");

    CheckWritesRestricted(writer);

    colWriter.Close();

    writer.WriteInt8("field2", 1);

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
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableCollectionReader<PortableInner> colReader = reader.ReadCollection<PortableInner>("field1");

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

void CheckCollection(CollectionType* colType)
{
    PortableInner writeVal1 = PortableInner(1);
    PortableInner writeVal2 = PortableInner(0);
    PortableInner writeVal3 = PortableInner(2);

    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    PortableCollectionWriter<PortableInner> colWriter = colType ?
        writer.WriteCollection<PortableInner>("field1", *colType) : writer.WriteCollection<PortableInner>("field1");

    colWriter.Write(writeVal1);
    colWriter.Write(writeVal2);
    colWriter.Write(writeVal3);

    CheckWritesRestricted(writer);

    colWriter.Close();

    writer.WriteInt8("field2", 1);

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
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableCollectionReader<PortableInner> colReader = reader.ReadCollection<PortableInner>("field1");

    CheckReadsRestricted(reader);

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

void CheckMapEmpty(MapType* mapType)
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    PortableMapWriter<int8_t, PortableInner> mapWriter = mapType ?
        writer.WriteMap<int8_t, PortableInner>("field1", *mapType) : writer.WriteMap<int8_t, PortableInner>("field1");

    CheckWritesRestricted(writer);

    mapWriter.Close();

    writer.WriteInt8("field2", 1);

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
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableMapReader<int8_t, PortableInner> mapReader = reader.ReadMap<int8_t, PortableInner>("field1");

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

void CheckMap(MapType* mapType)
{
    PortableInner writeVal1 = PortableInner(1);
    PortableInner writeVal2 = PortableInner(0);
    PortableInner writeVal3 = PortableInner(2);

    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    PortableMapWriter<int8_t, PortableInner> mapWriter = mapType ?
        writer.WriteMap<int8_t, PortableInner>("field1", *mapType) : writer.WriteMap<int8_t, PortableInner>("field1");

    mapWriter.Write(1, writeVal1);
    mapWriter.Write(2, writeVal2);
    mapWriter.Write(3, writeVal3);

    CheckWritesRestricted(writer);

    mapWriter.Close();

    writer.WriteInt8("field2", 1);

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
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableMapReader<int8_t, PortableInner> mapReader = reader.ReadMap<int8_t, PortableInner>("field1");

    CheckReadsRestricted(reader);

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_SUITE(PortableReaderWriterTestSuite)

BOOST_AUTO_TEST_CASE(TestPrimitiveInt8)
{
    CheckPrimitive<int8_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveBool)
{
    CheckPrimitive<bool>(true);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveInt16)
{
    CheckPrimitive<int16_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveUInt16)
{
    CheckPrimitive<uint16_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveInt32)
{
    CheckPrimitive<int32_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveInt64)
{
    CheckPrimitive<int64_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveFloat)
{
    CheckPrimitive<float>(1.1f);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveDouble)
{
    CheckPrimitive<double>(1.1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveGuid)
{
    Guid val(1, 2);

    CheckPrimitive<Guid>(val);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayInt8)
{
    CheckPrimitiveArray<int8_t>(1, 2, 3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayBool)
{
    CheckPrimitiveArray<bool>(false, true, false);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayInt16)
{
    CheckPrimitiveArray<int16_t>(1, 2, 3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayUInt16)
{
    CheckPrimitiveArray<uint16_t>(1, 2, 3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayInt32)
{
    CheckPrimitiveArray<int32_t>(1, 2, 3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayInt64)
{
    CheckPrimitiveArray<int64_t>(1, 2, 3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayFloat)
{
    CheckPrimitiveArray<float>(1.1f, 2.2f, 3.3f);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayDouble)
{
    CheckPrimitiveArray<double>(1.1, 2.2, 3.3);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayGuid)
{
    Guid dflt(1, 2);
    Guid val1(3, 4);
    Guid val2(5, 6);

    CheckPrimitiveArray<Guid>(dflt, val1, val2);
}

BOOST_AUTO_TEST_CASE(TestGuidNull)
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    try
    {
        writer.WriteNull(NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    writer.WriteNull("test");

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100);
    PortableReader reader(&readerImpl);
    
    in.Position(18);

    try
    {
        reader.ReadGuid(NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    Guid expVal;
    Guid actualVal = reader.ReadGuid("test");

    BOOST_REQUIRE(actualVal == expVal);
}

BOOST_AUTO_TEST_CASE(TestString) {
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    const char* writeVal1 = "testtest";
    const char* writeVal2 = "test";
    std::string writeVal3 = writeVal1;

    try
    {
        writer.WriteString(NULL, writeVal1);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        writer.WriteString(NULL, writeVal1, 4);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        writer.WriteString(NULL, writeVal3);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    writer.WriteString("field1", writeVal1);
    writer.WriteString("field2", writeVal1, 4);
    writer.WriteString("field3", writeVal3);
    writer.WriteString("field4", NULL);
    writer.WriteString("field5", NULL, 4);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    try
    {
        char nullCheckRes[9];

        reader.ReadString(NULL, nullCheckRes, 9);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    try
    {
        reader.ReadString(NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    char readVal1[9];
    char readVal2[5];
    
    BOOST_REQUIRE(reader.ReadString("field1", NULL, 0) == 8);
    BOOST_REQUIRE(reader.ReadString("field1", NULL, 8) == 8);
    BOOST_REQUIRE(reader.ReadString("field1", readVal1, 0) == 8);
    BOOST_REQUIRE(reader.ReadString("field1", readVal1, 4) == 8);

    BOOST_REQUIRE(reader.ReadString("field1", readVal1, 9) == 8);
    std::string writeVal1Str = writeVal1;
    std::string readVal1Str = readVal1;
    BOOST_REQUIRE(readVal1Str.compare(writeVal1Str) == 0);

    BOOST_REQUIRE(reader.ReadString("field2", readVal2, 5) == 4);
    std::string writeVal2Str = writeVal2;
    std::string readVal2Str = readVal2;
    BOOST_REQUIRE(readVal2Str.compare(writeVal2Str) == 0);

    std::string readVal3 = reader.ReadString("field3");
    BOOST_REQUIRE(readVal3.compare(writeVal3) == 0);

    BOOST_REQUIRE(reader.ReadString("field4", readVal1, 9) == -1);
    BOOST_REQUIRE(reader.ReadString("field5", readVal1, 9) == -1);
}

BOOST_AUTO_TEST_CASE(TestStringArrayNull)
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    writer.WriteNull("field1");
    writer.WriteInt8("field2", 1);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableStringArrayReader arrReader = reader.ReadStringArray("field1");

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestStringArrayEmpty)
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    PortableStringArrayWriter arrWriter = writer.WriteStringArray("field1");
    
    CheckWritesRestricted(writer);

    arrWriter.Close();

    writer.WriteInt8("field2", 1);

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
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableStringArrayReader arrReader = reader.ReadStringArray("field1");

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestStringArray)
{
    const char* writeVal1 = "testtest";
    const char* writeVal2 = "test";
    std::string writeVal3 = "test2";

    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    PortableStringArrayWriter arrWriter = writer.WriteStringArray("field1");

    arrWriter.Write(writeVal1);
    arrWriter.Write(writeVal1, 4);
    arrWriter.Write(NULL); // NULL value.
    arrWriter.Write(NULL, 100); // NULL value again.
    arrWriter.Write(writeVal3);

    CheckWritesRestricted(writer);

    arrWriter.Close();

    writer.WriteInt8("field2", 1);

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
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableStringArrayReader arrReader = reader.ReadStringArray("field1");

    CheckReadsRestricted(reader);

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestObject)
{
    PortableInner writeVal1(1);
    PortableInner writeVal2(0);

    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    writer.WriteObject("field1", writeVal1);
    writer.WriteObject("field2", writeVal2);
    writer.WriteNull("field3");

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableInner readVal1 = reader.ReadObject<PortableInner>("field1");
    BOOST_REQUIRE(writeVal1.GetValue() == readVal1.GetValue());

    PortableInner readVal2 = reader.ReadObject<PortableInner>("field2");
    BOOST_REQUIRE(writeVal2.GetValue() == readVal2.GetValue());

    PortableInner readVal3 = reader.ReadObject<PortableInner>("field3");
    BOOST_REQUIRE(0 == readVal3.GetValue());
}

BOOST_AUTO_TEST_CASE(TestNestedObject)
{
    PortableOuter writeVal1(1, 2);
    PortableOuter writeVal2(0, 0);

    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    writer.WriteObject("field1", writeVal1);
    writer.WriteObject("field2", writeVal2);
    writer.WriteNull("field3");

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableOuter readVal1 = reader.ReadObject<PortableOuter>("field1");
    BOOST_REQUIRE(writeVal1.GetValue() == readVal1.GetValue());
    BOOST_REQUIRE(writeVal1.GetInner().GetValue() == readVal1.GetInner().GetValue());

    PortableOuter readVal2 = reader.ReadObject<PortableOuter>("field2");
    BOOST_REQUIRE(writeVal2.GetValue() == readVal2.GetValue());
    BOOST_REQUIRE(writeVal2.GetInner().GetValue() == readVal2.GetInner().GetValue());

    PortableOuter readVal3 = reader.ReadObject<PortableOuter>("field3");
    BOOST_REQUIRE(0 == readVal3.GetValue());
    BOOST_REQUIRE(0 == readVal3.GetInner().GetValue());
}

BOOST_AUTO_TEST_CASE(TestArrayNull)
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    writer.WriteNull("field1");
    writer.WriteInt8("field2", 1);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableArrayReader<PortableInner> arrReader = reader.ReadArray<PortableInner>("field1");

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestArrayEmpty) 
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    PortableArrayWriter<PortableInner> arrWriter = writer.WriteArray<PortableInner>("field1");

    CheckWritesRestricted(writer);

    arrWriter.Close();

    writer.WriteInt8("field2", 1);

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
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableArrayReader<PortableInner> arrReader = reader.ReadArray<PortableInner>("field1");

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestArray)
{
    PortableInner writeVal1 = PortableInner(1);
    PortableInner writeVal2 = PortableInner(0);
    PortableInner writeVal3 = PortableInner(2);

    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    PortableArrayWriter<PortableInner> arrWriter = writer.WriteArray<PortableInner>("field1");

    arrWriter.Write(writeVal1); 
    arrWriter.Write(writeVal2);
    arrWriter.Write(writeVal3);

    CheckWritesRestricted(writer);

    arrWriter.Close();

    writer.WriteInt8("field2", 1);

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
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableArrayReader<PortableInner> arrReader = reader.ReadArray<PortableInner>("field1");

    CheckReadsRestricted(reader);

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestCollectionNull)
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    writer.WriteNull("field1");
    writer.WriteInt8("field2", 1);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableCollectionReader<PortableInner> colReader = reader.ReadCollection<PortableInner>("field1");

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestCollectionEmpty)
{
    CheckCollectionEmpty(NULL);
}

BOOST_AUTO_TEST_CASE(TestCollectionEmptyTyped)
{
    CollectionType typ = IGNITE_COLLECTION_CONCURRENT_SKIP_LIST_SET;

    CheckCollectionEmpty(&typ);
}

BOOST_AUTO_TEST_CASE(TestCollection)
{
    CheckCollection(NULL);
}

BOOST_AUTO_TEST_CASE(testCollectionTyped)
{
    CollectionType typ = IGNITE_COLLECTION_CONCURRENT_SKIP_LIST_SET;

    CheckCollection(&typ);
}

BOOST_AUTO_TEST_CASE(TestMapNull)
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    writer.WriteNull("field1");
    writer.WriteInt8("field2", 1);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 1000);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableMapReader<int8_t, PortableInner> mapReader = reader.ReadMap<int8_t, PortableInner>("field1");

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

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestMapEmpty)
{
    CheckMapEmpty(NULL);
}

BOOST_AUTO_TEST_CASE(TestMapEmptyTyped)
{
    MapType typ = IGNITE_MAP_CONCURRENT_HASH_MAP;

    CheckMapEmpty(&typ);
}

BOOST_AUTO_TEST_CASE(TestMap)
{
    CheckMap(NULL);
}

BOOST_AUTO_TEST_CASE(TestMapTyped)
{
    MapType typ = IGNITE_MAP_CONCURRENT_HASH_MAP;

    CheckMap(&typ);
}

BOOST_AUTO_TEST_CASE(TestRawMode)
{
    TemplatedPortableIdResolver<PortableDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writerImpl(&out, &idRslvr, NULL, NULL);
    PortableWriter writer(&writerImpl);

    out.Position(18);

    PortableRawWriter rawWriter = writer.RawWriter();

    try
    {
        writer.RawWriter();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    rawWriter.WriteInt8(1);

    CheckWritesRestricted(writer);

    out.Synchronize();

    InteropInputStream in(&mem);
    PortableReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, 18);
    PortableReader reader(&readerImpl);

    in.Position(18);

    PortableRawReader rawReader = reader.RawReader();

    try
    {
        reader.RawReader();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);

    CheckReadsRestricted(reader);
}

BOOST_AUTO_TEST_CASE(TestFieldSeek)
{
    TemplatedPortableIdResolver<PortableFields> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    PortableWriterImpl writer(&out, NULL);

    PortableFields writeVal(1, 2, 3, 4);

    writer.WriteTopObject<PortableFields>(writeVal);

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t pos = in.Position();
    in.ReadInt8(); // We do not need a header here.
    bool usrType = in.ReadBool();
    int32_t typeId = in.ReadInt32();
    int32_t hashCode = in.ReadInt32();
    int32_t len = in.ReadInt32();
    int32_t rawOff = in.ReadInt32();

    PortableReaderImpl readerImpl(&in, &idRslvr, pos, usrType, typeId, hashCode, len, rawOff);
    PortableReader reader(&readerImpl);

    // 1. Clockwise.
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);

    // 2. Counter closkwise.
    in.Position(18);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);

    // 3. Same field twice.
    in.Position(18);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);

    in.Position(18);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    
    // 4. Read missing field in between.
    in.Position(18);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);
    BOOST_REQUIRE(reader.ReadInt32("missing") == 0);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);

    in.Position(18);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    BOOST_REQUIRE(reader.ReadInt32("missing") == 0);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);

    // 5. Invalid field type.
    in.Position(18);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);

    try
    {
        reader.ReadInt64("val2");

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_PORTABLE);
    }

    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);

    // 6. Read missing primitive fields.
    BOOST_REQUIRE(reader.ReadInt8("missing") == 0);
    BOOST_REQUIRE(reader.ReadBool("missing") == false);
    BOOST_REQUIRE(reader.ReadInt16("missing") == 0);
    BOOST_REQUIRE(reader.ReadUInt16("missing") == 0);
    BOOST_REQUIRE(reader.ReadInt32("missing") == 0);
    BOOST_REQUIRE(reader.ReadInt64("missing") == 0);
    BOOST_REQUIRE(reader.ReadFloat("missing") == 0);
    BOOST_REQUIRE(reader.ReadDouble("missing") == 0);

    BOOST_REQUIRE(reader.ReadGuid("missing").GetMostSignificantBits() == 0);
    BOOST_REQUIRE(reader.ReadGuid("missing").GetLeastSignificantBits() == 0);

    // 7. Read missing primitive array fields.
    BOOST_REQUIRE(reader.ReadInt8Array("missing", NULL, 1) == -1);
    BOOST_REQUIRE(reader.ReadBoolArray("missing", NULL, 1) == -1);
    BOOST_REQUIRE(reader.ReadInt16Array("missing", NULL, 1) == -1);
    BOOST_REQUIRE(reader.ReadUInt16Array("missing", NULL, 1) == -1);
    BOOST_REQUIRE(reader.ReadInt32Array("missing", NULL, 1) == -1);
    BOOST_REQUIRE(reader.ReadInt64Array("missing", NULL, 1) == -1);
    BOOST_REQUIRE(reader.ReadFloatArray("missing", NULL, 1) == -1);
    BOOST_REQUIRE(reader.ReadDoubleArray("missing", NULL, 1) == -1);

    BOOST_REQUIRE(reader.ReadGuidArray("missing", NULL, 1) == -1);

    // 8. Read missing string fields.
    BOOST_REQUIRE(reader.ReadString("missing", NULL, 1) == -1);
    BOOST_REQUIRE(reader.ReadString("missing").length() == 0);

    // 9. Read missing object fields.
    BOOST_REQUIRE(reader.ReadObject<PortableFields*>("missing") == NULL);
    
    // 10. Read missing container fields.
    PortableStringArrayReader stringArrReader = reader.ReadStringArray("missing");
    BOOST_REQUIRE(stringArrReader.IsNull());

    PortableArrayReader<PortableFields> arrReader = reader.ReadArray<PortableFields>("missing");
    BOOST_REQUIRE(arrReader.IsNull());

    PortableCollectionReader<PortableFields> colReader = reader.ReadCollection<PortableFields>("missing");
    BOOST_REQUIRE(colReader.IsNull());

    PortableMapReader<int32_t, PortableFields> mapReader = reader.ReadMap<int32_t, PortableFields>("missing");
    BOOST_REQUIRE(mapReader.IsNull());
}

BOOST_AUTO_TEST_SUITE_END()