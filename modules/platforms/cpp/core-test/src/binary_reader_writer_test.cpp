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
#include "ignite/binary/binary.h"

#include "ignite/binary_test_defs.h"
#include "ignite/binary_test_utils.h"

using namespace ignite;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;
using namespace ignite::binary;
using namespace ignite_test::core::binary;

template<typename T>
void CheckPrimitive(T val)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    out.Position(IGNITE_DFLT_HDR_LEN);

    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    try
    {
        Write<T>(writer, NULL, val);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    Write<T>(writer, "test", val);

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    in.Synchronize();

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    try
    {
        Read<T>(reader, NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        T readVal = Read<T>(reader, "test"); 

        BOOST_REQUIRE(readVal == val);
    }
    catch (IgniteError& err)
    {
        BOOST_FAIL(err.GetText());
    }
}

template<typename T>
void CheckPrimitiveArray(T dflt, T val1, T val2)
{
    const char* fieldName = "test";

    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    T arr1[2];
    arr1[0] = dflt;
    arr1[1] = dflt;

    T arr2[2];
    arr2[0] = val1;
    arr2[1] = val2;

    {
        // 1. Write NULL and see what happens.
        WriteArray<T>(writer, fieldName, NULL, 0);

        writerImpl.PostWrite();

        out.Synchronize();

        InteropInputStream in(&mem);

        in.Synchronize();

        int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
        int32_t footerEnd = footerBegin + 5;

        BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
        BinaryReader reader(&readerImpl);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 0) == -1);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 2) == -1);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 1) == -1);

        BOOST_REQUIRE(arr1[0] == dflt);
        BOOST_REQUIRE(arr1[1] == dflt);
    }

    {
        // 2. Write empty array.
        out.Position(IGNITE_DFLT_HDR_LEN);

        WriteArray<T>(writer, fieldName, arr2, 0);

        writerImpl.PostWrite();

        out.Synchronize();

        InteropInputStream in(&mem);

        in.Synchronize();

        int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
        int32_t footerEnd = footerBegin + 5;

        BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
        BinaryReader reader(&readerImpl);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 0) == 0);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 2) == 0);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 0) == 0);
        BOOST_REQUIRE(arr1[0] == dflt);
        BOOST_REQUIRE(arr1[1] == dflt);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 2) == 0);
        BOOST_REQUIRE(arr1[0] == dflt);
        BOOST_REQUIRE(arr1[1] == dflt);
    }

    {
        // 3. Partial array write.
        out.Position(IGNITE_DFLT_HDR_LEN);

        WriteArray<T>(writer, fieldName, arr2, 1);

        writerImpl.PostWrite();

        out.Synchronize();

        InteropInputStream in(&mem);

        in.Synchronize();

        int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
        int32_t footerEnd = footerBegin + 5;

        BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
        BinaryReader reader(&readerImpl);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 0) == 1);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 2) == 1);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 0) == 1);
        BOOST_REQUIRE(arr1[0] == dflt);
        BOOST_REQUIRE(arr1[1] == dflt);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 1) == 1);
        BOOST_REQUIRE(arr1[0] == val1);
        BOOST_REQUIRE(arr1[1] == dflt);
        arr1[0] = dflt;

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, arr1, 2) == 1);
        BOOST_REQUIRE(arr1[0] == val1);
        BOOST_REQUIRE(arr1[1] == dflt);
        arr1[0] = dflt;
    }

    {
        // 4. Full array write.
        out.Position(IGNITE_DFLT_HDR_LEN);

        WriteArray<T>(writer, fieldName, arr2, 2);

        writerImpl.PostWrite();

        out.Synchronize();

        InteropInputStream in(&mem);

        in.Synchronize();

        int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
        int32_t footerEnd = footerBegin + 5;

        BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
        BinaryReader reader(&readerImpl);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 0) == 2);

        in.Position(IGNITE_DFLT_HDR_LEN);
        BOOST_REQUIRE(ReadArray<T>(reader, fieldName, NULL, 2) == 2);

        try
        {
            ReadArray<T>(reader, NULL, arr1, 2);

            BOOST_FAIL("Not restricted.");
        }
        catch (IgniteError& err)
        {
            BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
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
}

void CheckWritesRestricted(BinaryWriter& writer)
{
    try
    {
        writer.WriteInt8("field", 1);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        int8_t arr[1];

        writer.WriteInt8Array("field", arr, 1);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        Guid val(1, 1);

        writer.WriteGuid("field", val);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        Date val(1);

        writer.WriteDate("field", val);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        Timestamp val(1);

        writer.WriteTimestamp("field", val);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        writer.WriteString("field", "test");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try 
    {
        writer.WriteArray<int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try 
    {
        writer.WriteCollection<int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try 
    {
        writer.WriteMap<int8_t, int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }
}

void CheckReadsRestricted(BinaryReader& reader)
{
    try
    {
        reader.ReadInt8("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        int8_t arr[1];

        reader.ReadInt8Array("field", arr, 1);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        reader.ReadGuid("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        reader.ReadDate("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        reader.ReadTimestamp("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        reader.ReadString("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        reader.ReadArray<int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        reader.ReadCollection<int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        reader.ReadMap<int8_t, int8_t>("field");

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }
}

void CheckCollectionEmpty(CollectionType* colType)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    BinaryCollectionWriter<BinaryInner> colWriter = colType ?
        writer.WriteCollection<BinaryInner>("field1", *colType) : writer.WriteCollection<BinaryInner>("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        colWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryCollectionReader<BinaryInner> colReader = reader.ReadCollection<BinaryInner>("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

void CheckCollection(CollectionType* colType)
{
    BinaryInner writeVal1 = BinaryInner(1);
    BinaryInner writeVal2 = BinaryInner(0);
    BinaryInner writeVal3 = BinaryInner(2);

    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    BinaryCollectionWriter<BinaryInner> colWriter = colType ?
        writer.WriteCollection<BinaryInner>("field1", *colType) : writer.WriteCollection<BinaryInner>("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        colWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryCollectionReader<BinaryInner> colReader = reader.ReadCollection<BinaryInner>("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

void CheckCollectionIterators(CollectionType* colType)
{
    typedef std::vector<BinaryInner> BinaryInnerVector;
    BinaryInnerVector writeValues;

    writeValues.push_back(1);
    writeValues.push_back(0);
    writeValues.push_back(2);

    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    if (colType)
        writer.WriteCollection("field1", writeValues.begin(), writeValues.end(), *colType);
    else
        writer.WriteCollection("field1", writeValues.begin(), writeValues.end());
    
    writer.WriteInt8("field2", 1);

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BOOST_REQUIRE(reader.ReadCollectionSize("field1") == writeValues.size());

    CollectionType expectedCollectionType = colType ? *colType : IGNITE_COLLECTION_UNDEFINED;
    BOOST_REQUIRE(reader.ReadCollectionType("field1") == expectedCollectionType);

    BinaryInnerVector readValues;
    std::back_insert_iterator<BinaryInnerVector> readInsertIterator(readValues);

    reader.ReadCollection<BinaryInner>("field1", readInsertIterator);
    
    BOOST_REQUIRE(readValues.size() == 3);

    BOOST_REQUIRE(readValues[0].GetValue() == writeValues[0].GetValue());
    BOOST_REQUIRE(readValues[1].GetValue() == writeValues[1].GetValue());
    BOOST_REQUIRE(readValues[2].GetValue() == writeValues[2].GetValue());
    
    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

void CheckMapEmpty(MapType* mapType)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    BinaryMapWriter<int8_t, BinaryInner> mapWriter = mapType ?
        writer.WriteMap<int8_t, BinaryInner>("field1", *mapType) : writer.WriteMap<int8_t, BinaryInner>("field1");

    CheckWritesRestricted(writer);

    mapWriter.Close();

    writer.WriteInt8("field2", 1);

    try
    {
        mapWriter.Write(1, BinaryInner(1));

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        mapWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryMapReader<int8_t, BinaryInner> mapReader = reader.ReadMap<int8_t, BinaryInner>("field1");

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
        BinaryInner val;

        mapReader.GetNext(&key, &val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

void CheckMap(MapType* mapType)
{
    BinaryInner writeVal1 = BinaryInner(1);
    BinaryInner writeVal2 = BinaryInner(0);
    BinaryInner writeVal3 = BinaryInner(2);

    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    BinaryMapWriter<int8_t, BinaryInner> mapWriter = mapType ?
        writer.WriteMap<int8_t, BinaryInner>("field1", *mapType) : writer.WriteMap<int8_t, BinaryInner>("field1");

    mapWriter.Write(1, writeVal1);
    mapWriter.Write(2, writeVal2);
    mapWriter.Write(3, writeVal3);

    CheckWritesRestricted(writer);

    mapWriter.Close();

    writer.WriteInt8("field2", 1);

    try
    {
        mapWriter.Write(4, BinaryInner(4));

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        mapWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryMapReader<int8_t, BinaryInner> mapReader = reader.ReadMap<int8_t, BinaryInner>("field1");

    CheckReadsRestricted(reader);

    if (mapType)
        BOOST_REQUIRE(mapReader.GetType() == *mapType);
    else
        BOOST_REQUIRE(mapReader.GetType() == IGNITE_MAP_UNDEFINED);

    BOOST_REQUIRE(mapReader.GetSize() == 3);
    BOOST_REQUIRE(!mapReader.IsNull());

    int8_t key;
    BinaryInner val;

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_SUITE(BinaryReaderWriterTestSuite)

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

BOOST_AUTO_TEST_CASE(TestPrimitiveDate)
{
    Date val(time(NULL) * 1000);

    CheckPrimitive<Date>(val);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveTimestamp)
{
    Timestamp val(time(NULL), 0);

    CheckPrimitive<Timestamp>(val);
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

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayDate)
{
    Date dflt(1);
    Date val1(2);
    Date val2(3);

    CheckPrimitiveArray<Date>(dflt, val1, val2);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayTimestamp)
{
    Timestamp dflt(1);
    Timestamp val1(2);
    Timestamp val2(3);

    CheckPrimitiveArray<Timestamp>(dflt, val1, val2);
}

BOOST_AUTO_TEST_CASE(TestGuidNull)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    try
    {
        writer.WriteNull(NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writer.WriteNull("test");

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);
    
    in.Position(IGNITE_DFLT_HDR_LEN);

    try
    {
        reader.ReadGuid(NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    Guid expVal;
    Guid actualVal = reader.ReadGuid("test");

    BOOST_REQUIRE(actualVal == expVal);
}

BOOST_AUTO_TEST_CASE(TestDateNull)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    try
    {
        writer.WriteNull(NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writer.WriteNull("test");

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);
    
    in.Position(IGNITE_DFLT_HDR_LEN);

    try
    {
        reader.ReadDate(NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    Date expVal;
    Date actualVal = reader.ReadDate("test");

    BOOST_REQUIRE(actualVal == expVal);
}

BOOST_AUTO_TEST_CASE(TestTimestampNull)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    try
    {
        writer.WriteNull(NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writer.WriteNull("test");

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    try
    {
        reader.ReadTimestamp(NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    Timestamp expVal;
    Timestamp actualVal = reader.ReadTimestamp("test");

    BOOST_REQUIRE(actualVal == expVal);
}

BOOST_AUTO_TEST_CASE(TestString) {
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        writer.WriteString(NULL, writeVal1, 4);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        writer.WriteString(NULL, writeVal3);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writer.WriteString("field1", writeVal1);
    writer.WriteString("field2", writeVal1, 4);
    writer.WriteString("field3", writeVal3);
    writer.WriteString("field4", NULL);
    writer.WriteString("field5", NULL, 4);

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 5;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    try
    {
        char nullCheckRes[9];

        reader.ReadString(NULL, nullCheckRes, 9);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        reader.ReadString(NULL);

        BOOST_FAIL("Not restricted.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
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
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    writer.WriteNull("field1");
    writer.WriteInt8("field2", 1);

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryStringArrayReader arrReader = reader.ReadStringArray("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        arrReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestStringArrayEmpty)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    BinaryStringArrayWriter arrWriter = writer.WriteStringArray("field1");
    
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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        const char* val = "test";

        arrWriter.Write(val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        std::string val = "test";

        arrWriter.Write(val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        arrWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryStringArrayReader arrReader = reader.ReadStringArray("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        arrReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestStringArray)
{
    const char* writeVal1 = "testtest";
    const char* writeVal2 = "test";
    std::string writeVal3 = "test2";

    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    BinaryStringArrayWriter arrWriter = writer.WriteStringArray("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        const char* val = "test";

        arrWriter.Write(val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        std::string val = "test";

        arrWriter.Write(val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        arrWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryStringArrayReader arrReader = reader.ReadStringArray("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        arrReader.GetNext();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestObject)
{
    BinaryInner writeVal1(1);
    BinaryInner writeVal2(0);

    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    writer.WriteObject("field1", writeVal1);
    writer.WriteObject("field2", writeVal2);
    writer.WriteNull("field3");

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 3;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN); 

    BinaryInner readVal1 = reader.ReadObject<BinaryInner>("field1");

    BOOST_REQUIRE(writeVal1.GetValue() == readVal1.GetValue());

    BinaryInner readVal2 = reader.ReadObject<BinaryInner>("field2");
    BOOST_REQUIRE(writeVal2.GetValue() == readVal2.GetValue());

    BinaryInner readVal3 = reader.ReadObject<BinaryInner>("field3");
    BOOST_REQUIRE(0 == readVal3.GetValue());
}

BOOST_AUTO_TEST_CASE(TestNestedObject)
{
    BinaryOuter writeVal1(1, 2);
    BinaryOuter writeVal2(0, 0);

    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    writer.WriteObject("field1", writeVal1);
    writer.WriteObject("field2", writeVal2);
    writer.WriteNull("field3");

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 3;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryOuter readVal1 = reader.ReadObject<BinaryOuter>("field1");
    BOOST_REQUIRE(writeVal1.GetValue() == readVal1.GetValue());
    BOOST_REQUIRE(writeVal1.GetInner().GetValue() == readVal1.GetInner().GetValue());

    BinaryOuter readVal2 = reader.ReadObject<BinaryOuter>("field2");
    BOOST_REQUIRE(writeVal2.GetValue() == readVal2.GetValue());
    BOOST_REQUIRE(writeVal2.GetInner().GetValue() == readVal2.GetInner().GetValue());

    BinaryOuter readVal3 = reader.ReadObject<BinaryOuter>("field3");
    BOOST_REQUIRE(0 == readVal3.GetValue());
    BOOST_REQUIRE(0 == readVal3.GetInner().GetValue());
}

BOOST_AUTO_TEST_CASE(TestArrayNull)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    writer.WriteNull("field1");
    writer.WriteInt8("field2", 1);

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryArrayReader<BinaryInner> arrReader = reader.ReadArray<BinaryInner>("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestArrayEmpty) 
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    BinaryArrayWriter<BinaryInner> arrWriter = writer.WriteArray<BinaryInner>("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        arrWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryArrayReader<BinaryInner> arrReader = reader.ReadArray<BinaryInner>("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestArray)
{
    BinaryInner writeVal1 = BinaryInner(1);
    BinaryInner writeVal2 = BinaryInner(0);
    BinaryInner writeVal3 = BinaryInner(2);

    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    BinaryArrayWriter<BinaryInner> arrWriter = writer.WriteArray<BinaryInner>("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    try
    {
        arrWriter.Close();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryArrayReader<BinaryInner> arrReader = reader.ReadArray<BinaryInner>("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestCollectionNull)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    writer.WriteNull("field1");
    writer.WriteInt8("field2", 1);

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryCollectionReader<BinaryInner> colReader = reader.ReadCollection<BinaryInner>("field1");

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
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestCollectionEmpty)
{
    CheckCollectionEmpty(NULL);
}

BOOST_AUTO_TEST_CASE(TestCollectionEmptyTyped)
{
    CollectionType typ = IGNITE_COLLECTION_LINKED_HASH_SET;

    CheckCollectionEmpty(&typ);
}

BOOST_AUTO_TEST_CASE(TestCollection)
{
    CheckCollection(NULL);
}

BOOST_AUTO_TEST_CASE(testCollectionTyped)
{
    CollectionType typ = IGNITE_COLLECTION_LINKED_HASH_SET;

    CheckCollection(&typ);
}

BOOST_AUTO_TEST_CASE(TestCollectionIterators)
{
    CheckCollectionIterators(NULL);
}

BOOST_AUTO_TEST_CASE(TestCollectionIteratorsTyped)
{
    CollectionType typ = IGNITE_COLLECTION_LINKED_HASH_SET;

    CheckCollectionIterators(&typ);
}

BOOST_AUTO_TEST_CASE(TestMapNull)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    writer.WriteNull("field1");
    writer.WriteInt8("field2", 1);

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 5 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryMapReader<int8_t, BinaryInner> mapReader = reader.ReadMap<int8_t, BinaryInner>("field1");

    BOOST_REQUIRE(mapReader.GetType() == IGNITE_MAP_UNDEFINED);
    BOOST_REQUIRE(mapReader.GetSize() == -1);
    BOOST_REQUIRE(!mapReader.HasNext());
    BOOST_REQUIRE(mapReader.IsNull());

    try
    {
        int8_t key;
        BinaryInner val;

        mapReader.GetNext(&key, &val);

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(reader.ReadInt8("field2") == 1);
}

BOOST_AUTO_TEST_CASE(TestMapEmpty)
{
    CheckMapEmpty(NULL);
}

BOOST_AUTO_TEST_CASE(TestMapEmptyTyped)
{
    MapType typ = IGNITE_MAP_LINKED_HASH_MAP;

    CheckMapEmpty(&typ);
}

BOOST_AUTO_TEST_CASE(TestMap)
{
    CheckMap(NULL);
}

BOOST_AUTO_TEST_CASE(TestMapTyped)
{
    MapType typ = IGNITE_MAP_LINKED_HASH_MAP;

    CheckMap(&typ);
}

BOOST_AUTO_TEST_CASE(TestRawMode)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    BinaryRawWriter rawWriter = writer.RawWriter();

    try
    {
        writer.RawWriter();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    rawWriter.WriteInt8(1);

    CheckWritesRestricted(writer);

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 1000, footerBegin, footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);
    BinaryReader reader(&readerImpl);
    
    in.Position(IGNITE_DFLT_HDR_LEN);

    BinaryRawReader rawReader = reader.RawReader();

    try
    {
        reader.RawReader();

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
    }

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);

    CheckReadsRestricted(reader);
}

BOOST_AUTO_TEST_CASE(TestFieldSeek)
{
    TemplatedBinaryIdResolver<BinaryFields> idRslvr;

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);

    BinaryFields writeVal(1, 2, 3, 4);

    writer.WriteTopObject<BinaryFields>(writeVal);

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t pos = in.Position();
    in.ReadInt8(); // We do not need a header here.
    in.ReadInt8(); // We do not need proto ver here.

    int16_t flags = in.ReadInt16();
    int32_t typeId = in.ReadInt32();
    int32_t hashCode = in.ReadInt32();
    int32_t len = in.ReadInt32();

    in.ReadInt32(); // Ignoring Schema Id.

    int32_t schemaOrRawOff = in.ReadInt32();

    int32_t rawOff;
    int32_t footerBegin;

    if (flags & IGNITE_BINARY_FLAG_HAS_SCHEMA)
        footerBegin = schemaOrRawOff;
    else
        footerBegin = len;

    int32_t trailingBytes = (len - footerBegin) % 8;

    int32_t footerEnd = len - trailingBytes;

    if (trailingBytes)
        rawOff = in.ReadInt32(pos + len - 4);
    else
        rawOff = schemaOrRawOff;

    bool usrType = flags & IGNITE_BINARY_FLAG_USER_TYPE;

    footerBegin += pos;
    footerEnd += pos;

    BinaryReaderImpl readerImpl(&in, &idRslvr, pos, usrType, 
                                  typeId, hashCode, len, rawOff, 
                                  footerBegin, footerEnd, OFFSET_TYPE_ONE_BYTE);

    BinaryReader reader(&readerImpl);

    // 1. Clockwise.
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);

    // 2. Counter closkwise.
    in.Position(IGNITE_DFLT_HDR_LEN);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);

    // 3. Same field twice.
    in.Position(IGNITE_DFLT_HDR_LEN);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);

    in.Position(IGNITE_DFLT_HDR_LEN);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    
    // 4. Read missing field in between.
    in.Position(IGNITE_DFLT_HDR_LEN);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);
    BOOST_REQUIRE(reader.ReadInt32("missing") == 0);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);

    in.Position(IGNITE_DFLT_HDR_LEN);
    BOOST_REQUIRE(reader.ReadInt32("val2") == 2);
    BOOST_REQUIRE(reader.ReadInt32("missing") == 0);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);

    // 5. Invalid field type.
    in.Position(IGNITE_DFLT_HDR_LEN);
    BOOST_REQUIRE(reader.ReadInt32("val1") == 1);

    try
    {
        reader.ReadInt64("val2");

        BOOST_FAIL("Error expected.");
    }
    catch (IgniteError& err)
    {
        BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_BINARY);
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
    BOOST_REQUIRE(reader.ReadObject<BinaryFields*>("missing") == NULL);
    
    // 10. Read missing container fields.
    BinaryStringArrayReader stringArrReader = reader.ReadStringArray("missing");
    BOOST_REQUIRE(stringArrReader.IsNull());

    BinaryArrayReader<BinaryFields> arrReader = reader.ReadArray<BinaryFields>("missing");
    BOOST_REQUIRE(arrReader.IsNull());

    BinaryCollectionReader<BinaryFields> colReader = reader.ReadCollection<BinaryFields>("missing");
    BOOST_REQUIRE(colReader.IsNull());

    BinaryMapReader<int32_t, BinaryFields> mapReader = reader.ReadMap<int32_t, BinaryFields>("missing");
    BOOST_REQUIRE(mapReader.IsNull());
}

BOOST_AUTO_TEST_CASE(TestSchemaOffset2ByteFields)
{
    const int fieldsNum = 64;

    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(4096);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    for (int i = 0; i < fieldsNum; ++i)
    {
        std::stringstream tmp;
        tmp << "field" << i;

        writer.WriteInt32(tmp.str().c_str(), i * 10);
    }

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 6 * fieldsNum;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_TWO_BYTES);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    for (int i = 0; i < fieldsNum; ++i)
    {
        std::stringstream tmp;
        tmp << "field" << i;

        BOOST_REQUIRE(reader.ReadInt32(tmp.str().c_str()) == i * 10);
    }
}

BOOST_AUTO_TEST_CASE(TestSchemaOffset4ByteFields)
{
    const int fieldsNum = 0x10000 / 4;

    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024 * 1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    for (int i = 0; i < fieldsNum; ++i)
    {
        std::stringstream tmp;
        tmp << "field" << i;

        writer.WriteInt32(tmp.str().c_str(), i * 10);
    }

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 8 * fieldsNum;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_FOUR_BYTES);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    for (int i = 0; i < fieldsNum; ++i)
    {
        std::stringstream tmp;
        tmp << "field" << i;

        BOOST_REQUIRE(reader.ReadInt32(tmp.str().c_str()) == i * 10);
    }
}

BOOST_AUTO_TEST_CASE(TestSchemaOffset2ByteArray)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(4096);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    int8_t dummyArray[256] = {};

    writer.WriteInt8Array("field1", dummyArray, sizeof(dummyArray));
    writer.WriteInt32("field2", 42);

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 6 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_TWO_BYTES);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BOOST_REQUIRE(reader.ReadInt32("field2") == 42);
}

BOOST_AUTO_TEST_CASE(TestSchemaOffset4ByteArray)
{
    TemplatedBinaryIdResolver<BinaryDummy> idRslvr;

    InteropUnpooledMemory mem(1024 * 1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writerImpl(&out, &idRslvr, NULL, NULL, 0);
    BinaryWriter writer(&writerImpl);

    out.Position(IGNITE_DFLT_HDR_LEN);

    int8_t dummyArray[0x10000] = {};

    writer.WriteInt8Array("field1", dummyArray, sizeof(dummyArray));
    writer.WriteInt32("field2", 42);

    writerImpl.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);

    int32_t footerBegin = in.ReadInt32(IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
    int32_t footerEnd = footerBegin + 8 * 2;

    BinaryReaderImpl readerImpl(&in, &idRslvr, 0, true, idRslvr.GetTypeId(), 0, 100, 100, footerBegin, footerEnd, OFFSET_TYPE_FOUR_BYTES);
    BinaryReader reader(&readerImpl);

    in.Position(IGNITE_DFLT_HDR_LEN);

    BOOST_REQUIRE(reader.ReadInt32("field2") == 42);
}

BOOST_AUTO_TEST_SUITE_END()