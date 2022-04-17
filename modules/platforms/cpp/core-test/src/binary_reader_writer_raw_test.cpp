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
void CheckRawPrimitive(T val)
{
        InteropUnpooledMemory mem(1024);

        InteropOutputStream out(&mem);
        BinaryWriterImpl writer(&out, NULL);
        BinaryRawWriter rawWriter(&writer);

        Write<T>(rawWriter, val);

        out.Synchronize();

        InteropInputStream in(&mem);
        BinaryReaderImpl reader(&in);
        BinaryRawReader rawReader(&reader);

        T readVal = Read<T>(rawReader);

        BOOST_REQUIRE(readVal == val);
}

template<typename T>
void CheckObjectPrimitive(T val)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);

    writer.WriteObject<T>(val);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);

    reader.ReadObject<T>();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));
}

template<typename T>
void CheckRawPrimitiveArray(T dflt, T val1, T val2)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    // 1. Write NULL and see what happens.
    WriteArray<T>(rawWriter, NULL, 0);

    out.Synchronize();
    in.Synchronize();

    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 0) == -1);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 2) == -1);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    T arr1[2];
    arr1[0] = dflt;
    arr1[1] = dflt;

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 1) == -1);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

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
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 2) == 0);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 0) == 0);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 2) == 0);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    // 3. Partial array write.
    out.Position(0);
    in.Position(0);

    WriteArray<T>(rawWriter, arr2, 1);

    out.Synchronize();
    in.Synchronize();

    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 0) == 1);
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 2) == 1);
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 0) == 1);
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 1) == 1);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(arr1[0] == val1);
    BOOST_REQUIRE(arr1[1] == dflt);
    arr1[0] = dflt;

    in.Position(0);
    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 2) == 1);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));
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
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(ReadArray<T>(rawReader, NULL, 2) == 2);
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 0) == 2);
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 1) == 2);
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(arr1[0] == dflt);
    BOOST_REQUIRE(arr1[1] == dflt);

    BOOST_REQUIRE(ReadArray<T>(rawReader, arr1, 2) == 2);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(arr1[0] == val1);
    BOOST_REQUIRE(arr1[1] == val2);
}

void CheckRawWritesRestricted(BinaryRawWriter& writer)
{
    BOOST_CHECK_EXCEPTION(writer.WriteInt8(1), IgniteError, IsBinaryError);

    int8_t arr[1];
    BOOST_CHECK_EXCEPTION(writer.WriteInt8Array(arr, 1), IgniteError, IsBinaryError);

    Guid guid(1, 1);
    BOOST_CHECK_EXCEPTION(writer.WriteGuid(guid), IgniteError, IsBinaryError);

    Date date(1);
    BOOST_CHECK_EXCEPTION(writer.WriteDate(date), IgniteError, IsBinaryError);

    Time time(1);
    BOOST_CHECK_EXCEPTION(writer.WriteTime(time), IgniteError, IsBinaryError);

    Timestamp ts(1);
    BOOST_CHECK_EXCEPTION(writer.WriteTimestamp(ts), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(writer.WriteString("test"), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(writer.WriteArray<int8_t>(), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(writer.WriteCollection<int8_t>(), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION((writer.WriteMap<int8_t, int8_t>()), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(writer.WriteInt8(1), IgniteError, IsBinaryError);
}

void CheckRawReadsRestricted(BinaryRawReader& reader)
{
    BOOST_CHECK_EXCEPTION(reader.ReadInt8(), IgniteError, IsBinaryError);

    int8_t arr[1];
    BOOST_CHECK_EXCEPTION(reader.ReadInt8Array(arr, 1), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(reader.ReadGuid(), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(reader.ReadDate(), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(reader.ReadTimestamp(), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(reader.ReadString(), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(reader.ReadArray<int8_t>(), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(reader.ReadCollection<int8_t>(), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION((reader.ReadMap<int8_t, int8_t>()), IgniteError, IsBinaryError);
}

void CheckRawCollectionEmpty(CollectionType::Type* colType)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    BinaryCollectionWriter<BinaryInner> colWriter = colType ?
        rawWriter.WriteCollection<BinaryInner>(*colType) : rawWriter.WriteCollection<BinaryInner>();

    CheckRawWritesRestricted(rawWriter);

    colWriter.Close();

    rawWriter.WriteInt8(1);

    BOOST_CHECK_EXCEPTION(colWriter.Write(1), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(colWriter.Close(), IgniteError, IsBinaryError);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryCollectionReader<BinaryInner> colReader = rawReader.ReadCollection<BinaryInner>();

    if (colType)
        BOOST_REQUIRE(colReader.GetType() == *colType);
    else
        BOOST_REQUIRE(colReader.GetType() == CollectionType::UNDEFINED);

    BOOST_REQUIRE(colReader.GetSize() == 0);
    BOOST_REQUIRE(!colReader.HasNext());
    BOOST_REQUIRE(!colReader.IsNull());

    BOOST_CHECK_EXCEPTION(colReader.GetNext(), IgniteError, IsBinaryError);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

void CheckRawCollection(CollectionType::Type* colType)
{
    BinaryInner writeVal1 = BinaryInner(1);
    BinaryInner writeVal2 = BinaryInner(0);
    BinaryInner writeVal3 = BinaryInner(2);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    BinaryCollectionWriter<BinaryInner> colWriter = colType ?
        rawWriter.WriteCollection<BinaryInner>(*colType) : rawWriter.WriteCollection<BinaryInner>();

    colWriter.Write(writeVal1);
    colWriter.Write(writeVal2);
    colWriter.Write(writeVal3);

    CheckRawWritesRestricted(rawWriter);

    colWriter.Close();

    rawWriter.WriteInt8(1);

    BOOST_CHECK_EXCEPTION(colWriter.Write(1), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(colWriter.Close(), IgniteError, IsBinaryError);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryCollectionReader<BinaryInner> colReader = rawReader.ReadCollection<BinaryInner>();

    CheckRawReadsRestricted(rawReader);

    if (colType)
        BOOST_REQUIRE(colReader.GetType() == *colType);
    else
        BOOST_REQUIRE(colReader.GetType() == CollectionType::UNDEFINED);

    BOOST_REQUIRE(colReader.GetSize() == 3);
    BOOST_REQUIRE(!colReader.IsNull());

    BOOST_REQUIRE(colReader.HasNext());
    BOOST_REQUIRE(colReader.GetNext().GetValue() == writeVal1.GetValue());

    BOOST_REQUIRE(colReader.HasNext());
    BOOST_REQUIRE(colReader.GetNext().GetValue() == writeVal2.GetValue());

    BOOST_REQUIRE(colReader.HasNext());
    BOOST_REQUIRE(colReader.GetNext().GetValue() == writeVal3.GetValue());

    BOOST_REQUIRE(!colReader.HasNext());

    BOOST_CHECK_EXCEPTION(colReader.GetNext(), IgniteError, IsBinaryError);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

void CheckRawCollectionIterators(CollectionType::Type* colType)
{
    typedef std::vector<BinaryInner> BinaryInnerVector;

    BinaryInnerVector writeValues;
    writeValues.push_back(1);
    writeValues.push_back(0);
    writeValues.push_back(2);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    if (colType)
        rawWriter.WriteCollection(writeValues.begin(), writeValues.end(), *colType);
    else
        rawWriter.WriteCollection(writeValues.begin(), writeValues.end());

    rawWriter.WriteInt8(1);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    size_t collectionSize = rawReader.ReadCollectionSize();
    BOOST_REQUIRE(collectionSize == writeValues.size());

    if (colType)
        BOOST_REQUIRE(rawReader.ReadCollectionType() == *colType);
    else
        BOOST_REQUIRE(rawReader.ReadCollectionType() == CollectionType::UNDEFINED);

    BinaryInnerVector readValues(collectionSize);

    int32_t elementsRead = rawReader.ReadCollection<BinaryInner>(readValues.begin());
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(elementsRead == 3);

    BOOST_REQUIRE(readValues[0].GetValue() == writeValues[0].GetValue());
    BOOST_REQUIRE(readValues[1].GetValue() == writeValues[1].GetValue());
    BOOST_REQUIRE(readValues[2].GetValue() == writeValues[2].GetValue());

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

void CheckRawMapEmpty(MapType::Type* mapType)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    BinaryMapWriter<int8_t, BinaryInner> mapWriter = mapType ?
        rawWriter.WriteMap<int8_t, BinaryInner>(*mapType) : rawWriter.WriteMap<int8_t, BinaryInner>();

    CheckRawWritesRestricted(rawWriter);

    mapWriter.Close();

    rawWriter.WriteInt8(1);

    BOOST_CHECK_EXCEPTION(mapWriter.Write(1, BinaryInner(1)), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(mapWriter.Close(), IgniteError, IsBinaryError);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryMapReader<int8_t, BinaryInner> mapReader = rawReader.ReadMap<int8_t, BinaryInner>();

    if (mapType)
        BOOST_REQUIRE(mapReader.GetType() == *mapType);
    else
        BOOST_REQUIRE(mapReader.GetType() == MapType::UNDEFINED);

    BOOST_REQUIRE(mapReader.GetSize() == 0);
    BOOST_REQUIRE(!mapReader.HasNext());
    BOOST_REQUIRE(!mapReader.IsNull());

    int8_t key;
    BinaryInner val;
    BOOST_CHECK_EXCEPTION(mapReader.GetNext(key, val), IgniteError, IsBinaryError);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

void CheckRawMap(MapType::Type* mapType)
{
    BinaryInner writeVal1 = BinaryInner(1);
    BinaryInner writeVal2 = BinaryInner(0);
    BinaryInner writeVal3 = BinaryInner(2);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    BinaryMapWriter<int8_t, BinaryInner> mapWriter = mapType ?
        rawWriter.WriteMap<int8_t, BinaryInner>(*mapType) : rawWriter.WriteMap<int8_t, BinaryInner>();

    mapWriter.Write(1, writeVal1);
    mapWriter.Write(2, writeVal2);
    mapWriter.Write(3, writeVal3);

    CheckRawWritesRestricted(rawWriter);

    mapWriter.Close();

    rawWriter.WriteInt8(1);

    BOOST_CHECK_EXCEPTION(mapWriter.Write(4, BinaryInner(4)), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(mapWriter.Close(), IgniteError, IsBinaryError);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryMapReader<int8_t, BinaryInner> mapReader = rawReader.ReadMap<int8_t, BinaryInner>();

    CheckRawReadsRestricted(rawReader);

    if (mapType)
        BOOST_REQUIRE(mapReader.GetType() == *mapType);
    else
        BOOST_REQUIRE(mapReader.GetType() == MapType::UNDEFINED);

    BOOST_REQUIRE(mapReader.GetSize() == 3);
    BOOST_REQUIRE(!mapReader.IsNull());

    int8_t key;
    BinaryInner val;

    BOOST_REQUIRE(mapReader.HasNext());

    mapReader.GetNext(key, val);
    BOOST_REQUIRE(key == 1);
    BOOST_REQUIRE(val.GetValue() == writeVal1.GetValue());

    mapReader.GetNext(key, val);
    BOOST_REQUIRE(key == 2);
    BOOST_REQUIRE(val.GetValue() == writeVal2.GetValue());

    mapReader.GetNext(key, val);
    BOOST_REQUIRE(key == 3);
    BOOST_REQUIRE(val.GetValue() == writeVal3.GetValue());

    BOOST_REQUIRE(!mapReader.HasNext());

    BOOST_CHECK_EXCEPTION(mapReader.GetNext(key, val), IgniteError, IsBinaryError);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_SUITE(BinaryReaderWriterRawTestSuite)

BOOST_AUTO_TEST_CASE(TestPrimitiveInt8)
{
    CheckRawPrimitive<int8_t>(1);
    CheckObjectPrimitive<int8_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveBool)
{
    CheckRawPrimitive<bool>(true);
    CheckObjectPrimitive<bool>(true);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveInt16)
{
    CheckRawPrimitive<int16_t>(1);
    CheckObjectPrimitive<int16_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveUInt16)
{
    CheckRawPrimitive<uint16_t>(1);
    CheckObjectPrimitive<uint16_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveInt32)
{
    CheckRawPrimitive<int32_t>(1);
    CheckObjectPrimitive<int32_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveInt64)
{
    CheckRawPrimitive<int64_t>(1);
    CheckObjectPrimitive<int64_t>(1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveFloat)
{
    CheckRawPrimitive<float>(1.1f);
    CheckObjectPrimitive<float>(1.1f);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveDouble)
{
    CheckRawPrimitive<double>(1.1);
    CheckObjectPrimitive<double>(1.1);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveGuid)
{
    Guid val(1, 2);

    CheckRawPrimitive<Guid>(val);
    CheckObjectPrimitive<Guid>(val);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveDate)
{
    Date val(time(NULL) * 1000);

    CheckRawPrimitive<Date>(val);
    CheckObjectPrimitive<Date>(val);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveTime)
{
    Time val(time(NULL) * 1000);

    CheckRawPrimitive<Time>(val);
    CheckObjectPrimitive<Time>(val);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveTimestamp)
{
    Timestamp val(time(NULL), 0);

    CheckRawPrimitive<Timestamp>(val);
    CheckObjectPrimitive<Timestamp>(val);
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

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayDate)
{
    Date dflt(1);
    Date val1(2);
    Date val2(3);

    CheckRawPrimitiveArray<Date>(dflt, val1, val2);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayTime)
{
    Time dflt(1);
    Time val1(2);
    Time val2(3);

    CheckRawPrimitiveArray<Time>(dflt, val1, val2);
}

BOOST_AUTO_TEST_CASE(TestPrimitiveArrayTimestamp)
{
    Timestamp dflt(1);
    Timestamp val1(2);
    Timestamp val2(3);

    CheckRawPrimitiveArray<Timestamp>(dflt, val1, val2);
}

BOOST_AUTO_TEST_CASE(TestGuidNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    rawWriter.WriteNull();

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    Guid expVal;
    Guid actualVal = rawReader.ReadGuid();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(actualVal == expVal);
}

BOOST_AUTO_TEST_CASE(TestDateNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    rawWriter.WriteNull();

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    Date expVal;
    Date actualVal = rawReader.ReadDate();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(actualVal == expVal);
}

BOOST_AUTO_TEST_CASE(TestTimeNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    rawWriter.WriteNull();

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    Time expVal;
    Time actualVal = rawReader.ReadTime();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(actualVal == expVal);
}

BOOST_AUTO_TEST_CASE(TestTimestampNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    rawWriter.WriteNull();

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    Timestamp expVal;
    Timestamp actualVal = rawReader.ReadTimestamp();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(actualVal == expVal);
}

BOOST_AUTO_TEST_CASE(TestString)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

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
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    char readVal1[9];
    char readVal2[5];

    BOOST_REQUIRE(rawReader.ReadString(NULL, 0) == 8);
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(rawReader.ReadString(NULL, 8) == 8);
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(rawReader.ReadString(readVal1, 0) == 8);
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(rawReader.ReadString(readVal1, 4) == 8);
    BOOST_REQUIRE(!IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadString(readVal1, 9) == 8);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));
    std::string writeVal1Str = writeVal1;
    std::string readVal1Str = readVal1;
    BOOST_REQUIRE(readVal1Str.compare(writeVal1Str) == 0);

    int32_t prevPos = in.Position();
    BOOST_REQUIRE(rawReader.ReadString(readVal2, 5) == 4);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, prevPos));
    std::string writeVal2Str = writeVal2;
    std::string readVal2Str = readVal2;
    BOOST_REQUIRE(readVal2Str.compare(writeVal2Str) == 0);

    prevPos = in.Position();
    std::string readVal3 = rawReader.ReadString();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, prevPos));
    BOOST_REQUIRE(readVal3.compare(writeVal3) == 0);

    prevPos = in.Position();
    BOOST_REQUIRE(rawReader.ReadString(readVal1, 9) == -1);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, prevPos));

    prevPos = in.Position();
    BOOST_REQUIRE(rawReader.ReadString(readVal1, 9) == -1);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, prevPos));
}

BOOST_AUTO_TEST_CASE(TestStringArrayNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    rawWriter.WriteNull();
    rawWriter.WriteInt8(1);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryStringArrayReader arrReader = rawReader.ReadStringArray();

    BOOST_REQUIRE(arrReader.GetSize() == -1);
    BOOST_REQUIRE(!arrReader.HasNext());
    BOOST_REQUIRE(arrReader.IsNull());

    char res[100];
    BOOST_CHECK_EXCEPTION(arrReader.GetNext(res, 100), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(arrReader.GetNext(), IgniteError, IsBinaryError);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestStringArrayEmpty)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    BinaryStringArrayWriter arrWriter = rawWriter.WriteStringArray();

    CheckRawWritesRestricted(rawWriter);

    arrWriter.Close();

    rawWriter.WriteInt8(1);

    const char* val1 = "test";
    BOOST_CHECK_EXCEPTION(arrWriter.Write(val1, 4), IgniteError, IsBinaryError);
    BOOST_CHECK_EXCEPTION(arrWriter.Write(val1), IgniteError, IsBinaryError);

    std::string val2 = "test";
    BOOST_CHECK_EXCEPTION(arrWriter.Write(val2), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(arrWriter.Close(), IgniteError, IsBinaryError);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryStringArrayReader arrReader = rawReader.ReadStringArray();

    BOOST_REQUIRE(arrReader.GetSize() == 0);
    BOOST_REQUIRE(!arrReader.HasNext());
    BOOST_REQUIRE(!arrReader.IsNull());

    char res[100];
    BOOST_CHECK_EXCEPTION(arrReader.GetNext(res, 100), IgniteError, IsBinaryError);
    BOOST_CHECK_EXCEPTION(arrReader.GetNext(), IgniteError, IsBinaryError);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestStringArray)
{
    const char* writeVal1 = "testtest";
    const char* writeVal2 = "test";
    std::string writeVal3 = "test2";

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    BinaryStringArrayWriter arrWriter = rawWriter.WriteStringArray();

    arrWriter.Write(writeVal1);
    arrWriter.Write(writeVal1, 4);
    arrWriter.Write(NULL); // NULL value.
    arrWriter.Write(NULL, 100); // NULL value again.
    arrWriter.Write(writeVal3);

    CheckRawWritesRestricted(rawWriter);

    arrWriter.Close();

    rawWriter.WriteInt8(1);

    const char* val1 = "test";
    BOOST_CHECK_EXCEPTION(arrWriter.Write(val1, 4), IgniteError, IsBinaryError);
    BOOST_CHECK_EXCEPTION(arrWriter.Write(val1), IgniteError, IsBinaryError);

    std::string val2 = "test";
    BOOST_CHECK_EXCEPTION(arrWriter.Write(val2), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(arrWriter.Close(), IgniteError, IsBinaryError);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryStringArrayReader arrReader = rawReader.ReadStringArray();

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

    char res[100];
    BOOST_CHECK_EXCEPTION(arrReader.GetNext(res, 100), IgniteError, IsBinaryError);
    BOOST_CHECK_EXCEPTION(arrReader.GetNext(), IgniteError, IsBinaryError);

    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestObject)
{
    BinaryInner writeVal1(1);
    BinaryInner writeVal2(0);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    rawWriter.WriteObject(writeVal1);
    rawWriter.WriteObject(writeVal2);
    rawWriter.WriteNull();

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryInner readVal1 = rawReader.ReadObject<BinaryInner>();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(writeVal1.GetValue() == readVal1.GetValue());

    int32_t prevPos = in.Position();
    BinaryInner readVal2 = rawReader.ReadObject<BinaryInner>();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, prevPos));
    BOOST_REQUIRE(writeVal2.GetValue() == readVal2.GetValue());

    prevPos = in.Position();
    BinaryInner readVal3 = rawReader.ReadObject<BinaryInner>();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, prevPos));
    BOOST_REQUIRE(0 == readVal3.GetValue());
}

BOOST_AUTO_TEST_CASE(TestNestedObject)
{
    BinaryOuter writeVal1(1, 2);
    BinaryOuter writeVal2(0, 0);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    rawWriter.WriteObject(writeVal1);
    rawWriter.WriteObject(writeVal2);
    rawWriter.WriteNull();

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryOuter readVal1 = rawReader.ReadObject<BinaryOuter>();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));
    BOOST_REQUIRE(writeVal1.GetValue() == readVal1.GetValue());
    BOOST_REQUIRE(writeVal1.GetInner().GetValue() == readVal1.GetInner().GetValue());

    int32_t prevPos = in.Position();
    BinaryOuter readVal2 = rawReader.ReadObject<BinaryOuter>();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, prevPos));
    BOOST_REQUIRE(writeVal2.GetValue() == readVal2.GetValue());
    BOOST_REQUIRE(writeVal2.GetInner().GetValue() == readVal2.GetInner().GetValue());

    prevPos = in.Position();
    BinaryOuter readVal3 = rawReader.ReadObject<BinaryOuter>();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, prevPos));
    BOOST_REQUIRE(0 == readVal3.GetValue());
    BOOST_REQUIRE(0 == readVal3.GetInner().GetValue());
}

BOOST_AUTO_TEST_CASE(TestArrayNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    rawWriter.WriteNull();
    rawWriter.WriteInt8(1);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryArrayReader<BinaryInner> arrReader = rawReader.ReadArray<BinaryInner>();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(arrReader.GetSize() == -1);
    BOOST_REQUIRE(!arrReader.HasNext());
    BOOST_REQUIRE(arrReader.IsNull());

    BOOST_CHECK_EXCEPTION(arrReader.GetNext(), IgniteError, IsBinaryError);

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestArrayEmpty) 
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    BinaryArrayWriter<BinaryInner> arrWriter = rawWriter.WriteArray<BinaryInner>();

    CheckRawWritesRestricted(rawWriter);

    arrWriter.Close();

    rawWriter.WriteInt8(1);

    BOOST_CHECK_EXCEPTION(arrWriter.Write(1), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(arrWriter.Close(), IgniteError, IsBinaryError);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryArrayReader<BinaryInner> arrReader = rawReader.ReadArray<BinaryInner>();

    BOOST_REQUIRE(arrReader.GetSize() == 0);
    BOOST_REQUIRE(!arrReader.HasNext());
    BOOST_REQUIRE(!arrReader.IsNull());

    BOOST_CHECK_EXCEPTION(arrReader.GetNext(), IgniteError, IsBinaryError);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestArray)
{
    BinaryInner writeVal1 = BinaryInner(1);
    BinaryInner writeVal2 = BinaryInner(0);
    BinaryInner writeVal3 = BinaryInner(2);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    BinaryArrayWriter<BinaryInner> arrWriter = rawWriter.WriteArray<BinaryInner>();

    arrWriter.Write(writeVal1); 
    arrWriter.Write(writeVal2);
    arrWriter.Write(writeVal3);

    CheckRawWritesRestricted(rawWriter);

    arrWriter.Close();

    rawWriter.WriteInt8(1);

    BOOST_CHECK_EXCEPTION(arrWriter.Write(1), IgniteError, IsBinaryError);

    BOOST_CHECK_EXCEPTION(arrWriter.Close(), IgniteError, IsBinaryError);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryArrayReader<BinaryInner> arrReader = rawReader.ReadArray<BinaryInner>();

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

    BOOST_CHECK_EXCEPTION(arrReader.GetNext(), IgniteError, IsBinaryError);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestCollectionNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    rawWriter.WriteNull();
    rawWriter.WriteInt8(1);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryCollectionReader<BinaryInner> colReader = rawReader.ReadCollection<BinaryInner>();

    BOOST_REQUIRE(colReader.GetType() == CollectionType::UNDEFINED);
    BOOST_REQUIRE(colReader.GetSize() == -1);
    BOOST_REQUIRE(!colReader.HasNext());
    BOOST_REQUIRE(colReader.IsNull()); 

    BOOST_CHECK_EXCEPTION(colReader.GetNext(), IgniteError, IsBinaryError);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestCollectionEmpty)
{
    CheckRawCollectionEmpty(NULL);
}

BOOST_AUTO_TEST_CASE(TestCollectionEmptyTyped)
{
    CollectionType::Type typ = CollectionType::LINKED_HASH_SET;

    CheckRawCollectionEmpty(&typ);
}

BOOST_AUTO_TEST_CASE(TestCollection)
{
    CheckRawCollection(NULL);
}

BOOST_AUTO_TEST_CASE(TestCollectionTyped)
{
    CollectionType::Type typ = CollectionType::LINKED_HASH_SET;

    CheckRawCollection(&typ);
}

BOOST_AUTO_TEST_CASE(TestCollectionIterators)
{
    CheckRawCollectionIterators(NULL);
}

BOOST_AUTO_TEST_CASE(TestCollectionIteratorsTyped)
{
    CollectionType::Type typ = CollectionType::LINKED_HASH_SET;

    CheckRawCollectionIterators(&typ);
}

BOOST_AUTO_TEST_CASE(TestMapNull)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);
    BinaryRawWriter rawWriter(&writer);

    rawWriter.WriteNull();
    rawWriter.WriteInt8(1);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    BinaryMapReader<int8_t, BinaryInner> mapReader = rawReader.ReadMap<int8_t, BinaryInner>();

    BOOST_REQUIRE(mapReader.GetType() == MapType::UNDEFINED);
    BOOST_REQUIRE(mapReader.GetSize() == -1);
    BOOST_REQUIRE(!mapReader.HasNext());
    BOOST_REQUIRE(mapReader.IsNull());

    int8_t key;
    BinaryInner val;
    BOOST_CHECK_EXCEPTION(mapReader.GetNext(key, val), IgniteError, IsBinaryError);
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(rawReader.ReadInt8() == 1);
}

BOOST_AUTO_TEST_CASE(TestMapEmpty)
{
    CheckRawMapEmpty(NULL);
}

BOOST_AUTO_TEST_CASE(TestMapEmptyTyped)
{
    MapType::Type typ = MapType::LINKED_HASH_MAP;

    CheckRawMapEmpty(&typ);
}

BOOST_AUTO_TEST_CASE(TestMap)
{
    CheckRawMap(NULL);
}

BOOST_AUTO_TEST_CASE(TestMapTyped)
{
    MapType::Type typ = MapType::LINKED_HASH_MAP;

    CheckRawMap(&typ);
}

BOOST_AUTO_TEST_CASE(TestUserType)
{
    PureRaw expected("Hello Ignite from", 2017);

    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, NULL);

    writer.WriteObject<PureRaw>(expected);

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);

    PureRaw actual = reader.ReadObject<PureRaw>();
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in));

    BOOST_REQUIRE(actual == expected);
}

BOOST_AUTO_TEST_CASE(TestPrimitivePointers)
{
    InteropUnpooledMemory mem(1024);

    InteropOutputStream out(&mem);
    BinaryWriterImpl writer(&out, 0);
    BinaryRawWriter rawWriter(&writer);

    out.Position(IGNITE_DFLT_HDR_LEN);

    std::string field1 = "Lorem ipsum";
    int32_t field2 = 42;

    rawWriter.WriteObject(&field1);
    rawWriter.WriteObject<int8_t*>(0);
    rawWriter.WriteObject(&field2);

    writer.PostWrite();

    out.Synchronize();

    InteropInputStream in(&mem);
    BinaryReaderImpl reader(&in);
    BinaryRawReader rawReader(&reader);

    in.Position(IGNITE_DFLT_HDR_LEN);

    std::auto_ptr<std::string> field1Res(rawReader.ReadObject<std::string*>());
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, IGNITE_DFLT_HDR_LEN));

    int32_t prevPos = in.Position();
    std::auto_ptr<int8_t> fieldNullRes(rawReader.ReadObject<int8_t*>());
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, prevPos));

    prevPos = in.Position();
    std::auto_ptr<int32_t> field2Res(rawReader.ReadObject<int32_t*>());
    BOOST_REQUIRE(IsStreamPositionEqualOnSkip(in, prevPos));

    BOOST_CHECK_EQUAL(*field1Res, field1);
    BOOST_CHECK(fieldNullRes.get() == 0);
    BOOST_CHECK_EQUAL(*field2Res, field2);
}

BOOST_AUTO_TEST_SUITE_END()
