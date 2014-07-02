/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#ifndef _MSC_VER
#define BOOST_TEST_DYN_LINK
#endif

#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <iostream>
#include <string>
#include <forward_list>

#include <boost/shared_ptr.hpp>
#include <boost/test/unit_test.hpp>

#include <gridgain/gridgain.hpp>

#include "gridgain/gridclientvariant.hpp"
#include "gridgain/impl/marshaller/portable/gridportablemarshaller.hpp"

using namespace std;

BOOST_AUTO_TEST_SUITE(GridClientPortableMarshallerSuite)

class TestNonHashablePortable : public GridPortable {
public:
    int32_t typeId() const {
        return 0;
    }

    void writePortable(GridPortableWriter& writer) const {
        BOOST_FAIL("Should not be called");
    }

    void readPortable(GridPortableReader& reader) {
        BOOST_FAIL("Should not be called");
    }
};

class TestHashablePortable : public GridHashablePortable {
public:
    TestHashablePortable() {
    }

    TestHashablePortable(int32_t pId) : id(pId) {
    }

    int32_t typeId() const {
        return 10;
    }

    void writePortable(GridPortableWriter& writer) const {
        BOOST_FAIL("Should not be called");
    }

    void readPortable(GridPortableReader& reader) {
        BOOST_FAIL("Should not be called");
    }

    int hashCode() const {
        return id;
    }

    bool operator==(const GridHashablePortable& other) const {
        return id == static_cast<const TestHashablePortable*>(&other)->id;
    }

    int32_t id;
};

class TestHashablePortable2 : public GridHashablePortable {
public:
    TestHashablePortable2() {
    }

    TestHashablePortable2(int32_t pId) : id(pId) {
    }

    int32_t typeId() const {
        return 11;
    }

    void writePortable(GridPortableWriter& writer) const {
        BOOST_FAIL("Should not be called");
    }

    void readPortable(GridPortableReader& reader) {
        BOOST_FAIL("Should not be called");
    }

    int hashCode() const {
        return id;
    }

    bool operator==(const GridHashablePortable& other) const {
        return id == static_cast<const TestHashablePortable*>(&other)->id;
    }

    int32_t id;
};

class PortableAddress : public GridHashablePortable {
public:
    PortableAddress() {
    }

    PortableAddress(const string& addr) : addr(addr) {

    }
	int32_t typeId() const {
		return 10;
	}

    void writePortable(GridPortableWriter &writer) const {
        writer.writeString("addr", addr);
	}

    void readPortable(GridPortableReader &reader) {
		addr = reader.readString("addr").get_value_or(std::string());
	}

    bool operator==(const GridHashablePortable& portable) const {
        const PortableAddress* other = static_cast<const PortableAddress*>(&portable);

        return addr == other->addr;
    }

    int hashCode() const {
        return gridStringHash(addr);
    }

    string addr;
};

REGISTER_TYPE(PortableAddress);

class PortablePerson : public GridHashablePortable {
public:
    PortablePerson() : addr(0) {
    }

    PortablePerson(int id, string name, PortableAddress* addr) : name(name), id(id), addr(addr) {
    }

    PortablePerson(int id, string name) : name(name), id(id), addr(0) {
    }

    int32_t getId() {
        return id;
    }

    string& getName() {
        return name;
    }

	int32_t typeId() const {
		return 11;
	}

    void writePortable(GridPortableWriter& writer) const {
        writer.writeString("name", name);
		writer.writeInt32("id", id);
        
        if (addr)
            writer.writeVariant("addr", addr);
	}

    void readPortable(GridPortableReader& reader) {
		name = reader.readString("name").get_value_or(std::string());
        id = reader.readInt32("id");
        
        GridClientVariant addrVar = reader.readVariant("addr");

        if (addrVar.hasPortableObject())
            addr = addrVar.deserializePortable<PortableAddress>();
	}

    bool operator==(const GridHashablePortable& portable) const {
        const PortablePerson* other = static_cast<const PortablePerson*>(&portable);

        return id == other->id;
    }

    int hashCode() const {
        return id;
    }

private:
    string name;

    int32_t id;

    PortableAddress* addr;
};

REGISTER_TYPE(PortablePerson);

class Person {
public :
    Person(int32_t id, string name) : name(name), id(id) {
    }

    int32_t getId() {
        return id;
    }

    string getName() {
        return name;
    }

private:
    string name;

    int32_t id;
};

class TestPortable1 : public GridPortable {
public:
    TestPortable1() : arraysSize(-1), vDate(0) {
    }

    void writePortable(GridPortableWriter &writer) const {
        if (rawMarshalling) {
            GridPortableRawWriter& raw = writer.rawWriter();

            raw.writeBool(vBool);
            raw.writeBoolArray(vBoolArray, arraysSize);
            raw.writeBoolArray(vBoolVector.begin(), vBoolVector.end());

            raw.writeByte(vByte);
            raw.writeByteArray(vByteArray, arraysSize);
            raw.writeByteArray(vByteVector.begin(), vByteVector.end());

            raw.writeInt16(vInt16);
            raw.writeInt16Array(vInt16Array, arraysSize);
            raw.writeInt16Array(vInt16Vector.begin(), vInt16Vector.end());

            raw.writeInt32(vInt32);
            raw.writeInt32Array(vInt32Array, arraysSize);
            raw.writeInt32Array(vInt32Vector.begin(), vInt32Vector.end());

            raw.writeInt64(vInt64);
            raw.writeInt64Array(vInt64Array, arraysSize);
            raw.writeInt64Array(vInt64Vector.begin(), vInt64Vector.end());

            raw.writeFloat(vFloat);
            raw.writeFloatArray(vFloatArray, arraysSize);
            raw.writeFloatArray(vFloatVector.begin(), vFloatVector.end());

            raw.writeDouble(vDouble);
            raw.writeDoubleArray(vDoubleArray, arraysSize);
            raw.writeDoubleArray(vDoubleVector.begin(), vDoubleVector.end());

            raw.writeString(vStr);
            raw.writeStringArray(vStrVector.begin(), vStrVector.end());

            raw.writeWString(vWStr);
            raw.writeWStringArray(vWStrVector.begin(), vWStrVector.end());

            raw.writeVariant(vVariant);
            raw.writeVariantCollection(vVariantVector);
            raw.writeVariantMap(vVariantMap);

            raw.writeUuid(boost::optional<GridClientUuid>(vUuid));
            raw.writeUuidArray(vUuidVector.begin(), vUuidVector.end());

            raw.writeChar(vChar);
            raw.writeCharArray(vCharArray, arraysSize);
            raw.writeCharArray(vCharVector.begin(), vCharVector.end());

            raw.writeDate(vDate);
            raw.writeDateArray(vCharVector.begin(), vCharVector.end());

            raw.writeVariantArray(vVariantVector.begin(), vVariantVector.end());
        }
        else {
            writer.writeBool("1", vBool);
            writer.writeBoolArray("2", vBoolArray, arraysSize);
            writer.writeBoolArray("3", vBoolVector.begin(), vBoolVector.end());

            writer.writeByte("4", vByte);
            writer.writeByteArray("5", vByteArray, arraysSize);
            writer.writeByteArray("6", vByteVector.begin(), vByteVector.end());

            writer.writeInt16("7", vInt16);
            writer.writeInt16Array("8", vInt16Array, arraysSize);
            writer.writeInt16Array("9", vInt16Vector.begin(), vInt16Vector.end());

            writer.writeInt32("10", vInt32);
            writer.writeInt32Array("11", vInt32Array, arraysSize);
            writer.writeInt32Array("12", vInt32Vector.begin(), vInt32Vector.end());

            writer.writeInt64("13", vInt64);
            writer.writeInt64Array("14", vInt64Array, arraysSize);
            writer.writeInt64Array("15", vInt64Vector.begin(), vInt64Vector.end());

            writer.writeFloat("16", vFloat);
            writer.writeFloatArray("17", vFloatArray, arraysSize);
            writer.writeFloatArray("18", vFloatVector.begin(), vFloatVector.end());

            writer.writeDouble("19", vDouble);
            writer.writeDoubleArray("20", vDoubleArray, arraysSize);
            writer.writeDoubleArray("21", vDoubleVector.begin(), vDoubleVector.end());

            writer.writeString("22", vStr);
            writer.writeStringArray("23", vStrVector.begin(), vStrVector.end());

            writer.writeWString("24", vWStr);
            writer.writeWStringArray("25", vWStrVector.begin(), vWStrVector.end());

            writer.writeVariant("26", vVariant);
            writer.writeVariantCollection("27", vVariantVector);
            writer.writeVariantMap("28", vVariantMap);

            writer.writeUuid("29", vUuid);
            writer.writeUuidArray("30", vUuidVector.begin(), vUuidVector.end());

            writer.writeChar("31", vChar);
            writer.writeCharArray("32", vCharArray, arraysSize);
            writer.writeCharArray("33", vCharVector.begin(), vCharVector.end());

            writer.writeDate("34", vDate);
            writer.writeDateArray("35", vDateVector.begin(), vDateVector.end());

            writer.writeVariantArray("36", vVariantArray.begin(), vVariantArray.end());
        }
	}

    void readPortable(GridPortableReader &reader) {
        BOOST_REQUIRE_EQUAL(-1, arraysSize);

        if (rawMarshalling) {
            GridPortableRawReader& raw = reader.rawReader();

            vBool = raw.readBool();
            pair<bool*, int32_t> boolArr = raw.readBoolArray();
            arraysSize = boolArr.second;
            vBoolArray = boolArr.first;
            raw.readBoolArray(vBoolVector);

            vByte = raw.readByte();
            pair<int8_t*, int32_t> byteArr = raw.readByteArray();
            BOOST_REQUIRE_EQUAL(arraysSize, byteArr.second);
            vByteArray = byteArr.first;
            raw.readByteArray(vByteVector);

            vInt16 = raw.readInt16();
            pair<int16_t*, int32_t> int16Arr = raw.readInt16Array();
            BOOST_REQUIRE_EQUAL(arraysSize, int16Arr.second);
            vInt16Array = int16Arr.first;
            raw.readInt16Array(vInt16Vector);

            vInt32 = raw.readInt32();
            pair<int32_t*, int32_t> int32Arr = raw.readInt32Array();
            BOOST_REQUIRE_EQUAL(arraysSize, int32Arr.second);
            vInt32Array = int32Arr.first;
            raw.readInt32Array(vInt32Vector);

            vInt64 = raw.readInt64();
            pair<int64_t*, int32_t> int64Arr = raw.readInt64Array();
            BOOST_REQUIRE_EQUAL(arraysSize, int64Arr.second);
            vInt64Array = int64Arr.first;
            raw.readInt64Array(vInt64Vector);

            vFloat = raw.readFloat();
            pair<float*, int32_t> floatArr = raw.readFloatArray();
            BOOST_REQUIRE_EQUAL(arraysSize, floatArr.second);
            vFloatArray = floatArr.first;
            raw.readFloatArray(vFloatVector);

            vDouble = raw.readDouble();
            pair<double*, int32_t> doubleArr = raw.readDoubleArray();
            BOOST_REQUIRE_EQUAL(arraysSize, doubleArr.second);
            vDoubleArray = doubleArr.first;
            raw.readDoubleArray(vDoubleVector);

            vStr = string(raw.readString().get());
            raw.readStringArray(vStrVector);
            BOOST_REQUIRE_EQUAL(arraysSize, vStrVector.size());

            vWStr = wstring(raw.readWString().get());
            raw.readWStringArray(vWStrVector);
            BOOST_REQUIRE_EQUAL(arraysSize, vWStrVector.size());

            vVariant = raw.readVariant();

            raw.readVariantCollection(vVariantVector);
            BOOST_REQUIRE_EQUAL(arraysSize, vVariantVector.size());

            raw.readVariantMap(vVariantMap);
            BOOST_REQUIRE_EQUAL(arraysSize, vVariantMap.size());

            vUuid = GridClientUuid(raw.readUuid().get());
            raw.readUuidArray(vUuidVector);

            vChar = raw.readChar();
            pair<uint16_t*, int32_t> charArr = raw.readCharArray();
            BOOST_REQUIRE_EQUAL(arraysSize, charArr.second);
            vCharArray = charArr.first;
            raw.readCharArray(vCharVector);

            vDate = GridClientDate(raw.readDate().get());
            raw.readDateArray(vDateVector);

            raw.readVariantArray(vVariantArray);
            BOOST_REQUIRE_EQUAL(arraysSize, vVariantArray.size());
        }
        else {
            vBool = reader.readBool("1");
            pair<bool*, int32_t> boolArr = reader.readBoolArray("2");
            reader.readBoolArray("3", vBoolVector);
            vBoolArray = boolArr.first;
            arraysSize = boolArr.second;

            vByte = reader.readByte("4");
            pair<int8_t*, int32_t> byteArr = reader.readByteArray("5");
            BOOST_REQUIRE_EQUAL(arraysSize, byteArr.second);
            vByteArray = byteArr.first;
            reader.readByteArray("6", vByteVector);

            vInt16 = reader.readInt16("7");
            pair<int16_t*, int32_t> int16Arr = reader.readInt16Array("8");
            BOOST_REQUIRE_EQUAL(arraysSize, int16Arr.second);
            vInt16Array = int16Arr.first;
            reader.readInt16Array("9", vInt16Vector);

            vInt32 = reader.readInt32("10");
            pair<int32_t*, int32_t> int32Arr = reader.readInt32Array("11");
            BOOST_REQUIRE_EQUAL(arraysSize, int32Arr.second);
            vInt32Array = int32Arr.first;
            reader.readInt32Array("12", vInt32Vector);

            vInt64 = reader.readInt64("13");
            pair<int64_t*, int32_t> int64Arr = reader.readInt64Array("14");
            BOOST_REQUIRE_EQUAL(arraysSize, int64Arr.second);
            vInt64Array = int64Arr.first;
            reader.readInt64Array("15", vInt64Vector);

            vFloat = reader.readFloat("16");
            pair<float*, int32_t> floatArr = reader.readFloatArray("17");
            BOOST_REQUIRE_EQUAL(arraysSize, floatArr.second);
            vFloatArray = floatArr.first;
            reader.readFloatArray("18", vFloatVector);

            vDouble = reader.readDouble("19");
            pair<double*, int32_t> doubleArr = reader.readDoubleArray("20");
            BOOST_REQUIRE_EQUAL(arraysSize, doubleArr.second);
            vDoubleArray = doubleArr.first;
            reader.readDoubleArray("21", vDoubleVector);

            vStr = string(reader.readString("22").get());
            reader.readStringArray("23", vStrVector);

            vWStr = wstring(reader.readWString("24").get());
            reader.readWStringArray("25", vWStrVector);

            vVariant = reader.readVariant("26");
            reader.readVariantCollection("27", vVariantVector);
            reader.readVariantMap("28", vVariantMap);

            vUuid = GridClientUuid(reader.readUuid("29").get());
            reader.readUuidArray("30", vUuidVector);

            vChar = reader.readChar("31");
            pair<uint16_t*, int32_t> charArr = reader.readCharArray("32");
            BOOST_REQUIRE_EQUAL(arraysSize, charArr.second);
            vCharArray = charArr.first;
            reader.readCharArray("33", vCharVector);

            vDate = GridClientDate(reader.readDate("34").get());
            reader.readDateArray("35", vDateVector);
            
            reader.readVariantArray("36", vVariantArray);
        }
	}

    int32_t typeId() const {
        return 30;
    }

    static bool rawMarshalling;

    int32_t arraysSize;

    bool vBool;

    bool* vBoolArray;

    vector<bool> vBoolVector;

    int8_t vByte;

    int8_t* vByteArray;

    vector<int8_t> vByteVector;

    int16_t vInt16;

    int16_t* vInt16Array;

    vector<int16_t> vInt16Vector;

    int32_t vInt32;

    int32_t* vInt32Array;

    vector<int32_t> vInt32Vector;

    uint16_t vChar;

    uint16_t* vCharArray;

    vector<uint16_t> vCharVector;

    int64_t vInt64;

    int64_t* vInt64Array;

    vector<int64_t> vInt64Vector;

    float vFloat;

    float* vFloatArray;

    vector<float> vFloatVector;

    double vDouble;

    double* vDoubleArray;

    vector<double> vDoubleVector;

    string vStr;

    vector<string> vStrVector;

    wstring vWStr;

    vector<wstring> vWStrVector;

    GridClientUuid vUuid;

    vector<GridClientUuid> vUuidVector;

    GridClientDate vDate;

    vector<GridClientDate> vDateVector;

    GridClientVariant vVariant;

    vector<GridClientVariant> vVariantArray;

    vector<GridClientVariant> vVariantVector;

    TGridClientVariantMap vVariantMap;
};

bool TestPortable1::rawMarshalling = false;

REGISTER_TYPE(TestPortable1);

TestPortable1 createTestPortable1(int32_t arraysSize) {
    TestPortable1 p;

    p.arraysSize = arraysSize;

    p.vBool = true;
    p.vBoolArray = new bool[arraysSize];
    for (int i = 0; i < arraysSize; i++)
        p.vBoolArray[i] = i % 2 == 0;
    p.vBoolVector = vector<bool>(arraysSize, true);

    p.vByte = 1;
    p.vByteArray = new int8_t[arraysSize];
    for (int i = 0; i < arraysSize; i++)
        p.vByteArray[i] = i % 2 == 0 ? 1 : -1;
    p.vByteVector = vector<int8_t>(arraysSize, -1);

    p.vInt16 = 1;
    p.vInt16Array = new int16_t[arraysSize];
    for (int i = 0; i < arraysSize; i++)
        p.vInt16Array[i] = i % 2 == 0 ? 1 : -1;
    p.vInt16Vector = vector<int16_t>(arraysSize, -1);

    p.vInt32 = 1;
    p.vInt32Array = new int32_t[arraysSize];
    for (int i = 0; i < arraysSize; i++)
        p.vInt32Array[i] = i % 2 == 0 ? 1 : -1;
    p.vInt32Vector = vector<int32_t>(arraysSize, -1);

    p.vChar = 1;
    p.vCharArray = new uint16_t[arraysSize];
    for (int i = 0; i < arraysSize; i++)
        p.vCharArray[i] = i % 2 == 0 ? 1 : 2;
    p.vCharVector = vector<uint16_t>(arraysSize, 2);

    p.vInt64 = 1;
    p.vInt64Array = new int64_t[arraysSize];
    for (int i = 0; i < arraysSize; i++)
        p.vInt64Array[i] = i % 2 == 0 ? 1 : -1;
    p.vInt64Vector = vector<int64_t>(arraysSize, -1);

    p.vFloat = 1.5;
    p.vFloatArray = new float[arraysSize];
    for (int i = 0; i < arraysSize; i++)
        p.vFloatArray[i] = i % 2 == 0 ? 1.5f : -1.5f;
    p.vFloatVector = vector<float>(arraysSize, -1);

    p.vDouble = 1.5;
    p.vDoubleArray = new double[arraysSize];
    for (int i = 0; i < arraysSize; i++)
        p.vDoubleArray[i] = i % 2 == 0 ? 1.5 : -1.5;
    p.vDoubleVector = vector<double>(arraysSize, -1);

    p.vStr = "str1";
    p.vStrVector = vector<string>(arraysSize, "str2");

    p.vWStr = L"wstr1";
    p.vWStrVector = vector<wstring>(arraysSize, L"wstr2");

    p.vUuid = GridClientUuid(1, 2);
    p.vUuidVector = vector<GridClientUuid>(arraysSize, GridClientUuid(3, 4));

    p.vDate = GridClientDate(1);
    p.vDateVector = vector<GridClientDate>(arraysSize, GridClientDate(2));

    p.vVariant = GridClientVariant(1);
    p.vVariantVector = vector<GridClientVariant>(arraysSize, GridClientVariant(2));
    p.vVariantArray = vector<GridClientVariant>(arraysSize, GridClientVariant(3));

    p.vVariantMap = TGridClientVariantMap();

    for (int i = 0; i < arraysSize; i++)
        p.vVariantMap[GridClientVariant(i)] = GridClientVariant(i);

    return p;
}

void validateTestPortable1(TestPortable1 p, int32_t arraysSize) {
    BOOST_REQUIRE_EQUAL(arraysSize, p.arraysSize);

    BOOST_REQUIRE_EQUAL(true, p.vBool);
    for (int i = 0; i < arraysSize; i++) {
        bool exp = i % 2 == 0;

        if (exp != p.vBoolArray[i])
            BOOST_REQUIRE_EQUAL(exp, p.vBoolArray[i]);

        if (!p.vBoolVector[i])
            BOOST_REQUIRE_EQUAL(true, p.vBoolVector[i]);
    }

    BOOST_REQUIRE_EQUAL(1, p.vByte);
    for (int i = 0; i < arraysSize; i++) {
        int8_t exp = i % 2 == 0 ? 1 : -1;

        if (exp != p.vByteArray[i])
            BOOST_REQUIRE_EQUAL(exp, p.vByteArray[i]);

        if (-1 != p.vByteVector[i])
            BOOST_REQUIRE_EQUAL(-1, p.vByteVector[i]);
    }

    BOOST_REQUIRE_EQUAL(1, p.vInt16);
    for (int i = 0; i < arraysSize; i++) {
        int16_t exp = i % 2 == 0 ? 1 : -1;

        if (exp != p.vInt16Array[i])
            BOOST_REQUIRE_EQUAL(exp, p.vInt16Array[i]);

        if (-1 != p.vInt16Vector[i])
            BOOST_REQUIRE_EQUAL(-1, p.vInt16Vector[i]);
    }

    BOOST_REQUIRE_EQUAL(1, p.vInt32);
    for (int i = 0; i < arraysSize; i++) {
        int32_t exp = i % 2 == 0 ? 1 : -1;

        if (exp != p.vInt32Array[i])
            BOOST_REQUIRE_EQUAL(exp, p.vInt32Array[i]);

        if (-1 != p.vInt32Vector[i])
            BOOST_REQUIRE_EQUAL(-1, p.vInt32Vector[i]);
    }

    BOOST_REQUIRE_EQUAL(1, p.vChar);
    for (int i = 0; i < arraysSize; i++) {
        uint16_t exp = i % 2 == 0 ? 1 : 2;

        if (exp != p.vCharArray[i])
            BOOST_REQUIRE_EQUAL(exp, p.vCharArray[i]);

        if (2 != p.vCharVector[i])
            BOOST_REQUIRE_EQUAL(2, p.vCharVector[i]);
    }

    BOOST_REQUIRE_EQUAL(1, p.vInt64);
    for (int i = 0; i < arraysSize; i++) {
        int64_t exp = i % 2 == 0 ? 1 : -1;

        if (exp != p.vInt64Array[i])
            BOOST_REQUIRE_EQUAL(exp, p.vInt64Array[i]);

        if (-1 != p.vInt64Vector[i])
            BOOST_REQUIRE_EQUAL(-1, p.vInt64Vector[i]);
    }

    BOOST_REQUIRE_EQUAL(1.5, p.vFloat);
    for (int i = 0; i < arraysSize; i++) {
        float exp = i % 2 == 0 ? 1.5f : -1.5f;

        if (exp != p.vFloatArray[i])
            BOOST_REQUIRE_EQUAL(exp, p.vFloatArray[i]);

        if (-1 != p.vFloatVector[i])
            BOOST_REQUIRE_EQUAL(-1, p.vFloatVector[i]);
    }

    BOOST_REQUIRE_EQUAL(1.5, p.vDouble);
    for (int i = 0; i < arraysSize; i++) {
        double exp = i % 2 == 0 ? 1.5 : -1.5;

        if (exp != p.vDoubleArray[i])
            BOOST_REQUIRE_EQUAL(exp, exp);

        if (-1 != p.vDoubleVector[i])
            BOOST_REQUIRE_EQUAL(-1, p.vDoubleVector[i]);
    }

    BOOST_REQUIRE_EQUAL("str1", p.vStr);

    BOOST_REQUIRE_EQUAL(arraysSize, p.vStrVector.size());

    for (int i = 0; i < arraysSize; i++) {
        if ("str2" != p.vStrVector[i])
            BOOST_REQUIRE_EQUAL("str2", p.vStrVector[i]);
    }

    BOOST_REQUIRE(L"wstr1" == p.vWStr);

    BOOST_REQUIRE_EQUAL(arraysSize, p.vWStrVector.size());

    for (int i = 0; i < arraysSize; i++) {
        if (L"wstr2" != p.vWStrVector[i])
            BOOST_REQUIRE(L"wstr2" == p.vWStrVector[i]);
    }

    BOOST_REQUIRE_EQUAL(1, p.vUuid.mostSignificantBits());
    BOOST_REQUIRE_EQUAL(2, p.vUuid.leastSignificantBits());

    BOOST_REQUIRE_EQUAL(arraysSize, p.vUuidVector.size());

    for (int i = 0; i < arraysSize; i++) {
        if (3 != p.vUuidVector[i].mostSignificantBits())
            BOOST_REQUIRE_EQUAL(3, p.vUuidVector[i].mostSignificantBits());

        if (4 != p.vUuidVector[i].leastSignificantBits())
            BOOST_REQUIRE_EQUAL(4, p.vUuidVector[i].leastSignificantBits());
    }

    BOOST_REQUIRE_EQUAL(1, p.vDate.getTime());

    BOOST_REQUIRE_EQUAL(arraysSize, p.vDateVector.size());

    for (int i = 0; i < arraysSize; i++) {
        if (2 != p.vDateVector[i].getTime())
            BOOST_REQUIRE_EQUAL(2, p.vDateVector[i].getTime());
    }

    BOOST_REQUIRE_EQUAL(true, p.vVariant.hasInt());

    BOOST_REQUIRE_EQUAL(1, p.vVariant.getInt());

    BOOST_REQUIRE_EQUAL(arraysSize, p.vVariantVector.size());

    BOOST_REQUIRE_EQUAL(arraysSize, p.vVariantArray.size());

    for (int i = 0; i < arraysSize; i++) {
        if (2 != p.vVariantVector[i].getInt())
            BOOST_REQUIRE_EQUAL(2, p.vVariantVector[i].getInt());

        if (3 != p.vVariantArray[i].getInt())
            BOOST_REQUIRE_EQUAL(2, p.vVariantArray[i].getInt());
    }

    BOOST_REQUIRE_EQUAL(arraysSize, p.vVariantMap.size());

    for (int i = 0; i < arraysSize; i++) {
        GridClientVariant val = p.vVariantMap[GridClientVariant(i)];

        if (!val.hasInt())
            BOOST_REQUIRE_EQUAL(true, val.hasInt());

        if (i != val.getInt())
            BOOST_REQUIRE_EQUAL(i, val.getInt());
    }
}

void testPortable1Marshalling(bool rawMarshalling, int32_t arraysSize) {
    GridPortableMarshaller marsh;

    TestPortable1 p = createTestPortable1(arraysSize);

    TestPortable1::rawMarshalling = rawMarshalling;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(GridClientVariant(&p));

    TestPortable1* pRead = marsh.unmarshal(bytes, false).getPortable<TestPortable1>();

    validateTestPortable1(*pRead, arraysSize);

    delete pRead;
}

BOOST_AUTO_TEST_CASE(testPortableSerialization_allTypes) {
    testPortable1Marshalling(false, 10);
    testPortable1Marshalling(false, 1000);
    testPortable1Marshalling(false, 0);

    testPortable1Marshalling(true, 10);
    testPortable1Marshalling(true, 1000);
    testPortable1Marshalling(true, 0);
}

class TestPortableCycle2;

class TestPortableCycle1 : public GridPortable {
public:
    int32_t typeId() const {
        return 40;
    }

    void writePortable(GridPortableWriter& writer) const {
        writer.writeInt32("1", val1);
        writer.writeVariant("2", (GridPortable*)p2);
    }

    void readPortable(GridPortableReader& reader) {
        val1 = reader.readInt32("1");
        p2 = reader.readVariant("2").getPortable<TestPortableCycle2>();
    }

    TestPortableCycle2* p2;

    int32_t val1;
};

REGISTER_TYPE(TestPortableCycle1);

class TestPortableCycle2 : public GridPortable {
public:
    int32_t typeId() const {
        return 41;
    }

    void writePortable(GridPortableWriter& writer) const {
        writer.writeFloat("1", val1);
        writer.writeVariant("2", p1);
    }

    void readPortable(GridPortableReader& reader) {
        val1 = reader.readFloat("1");
        p1 = reader.readVariant("2").getPortable<TestPortableCycle1>();
    }

    TestPortableCycle1* p1;

    float val1;
};

REGISTER_TYPE(TestPortableCycle2);

BOOST_AUTO_TEST_CASE(testPortableSerialization_cycle) {
    GridPortableMarshaller marsh;

    TestPortableCycle1* p1 = new TestPortableCycle1();

    p1->val1 = 10;

    TestPortableCycle2* p2 = new TestPortableCycle2();

    p2->val1 = 10.5;

    p1->p2 = p2;
    p2->p1 = p1;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(GridClientVariant(p1));

    delete p1;
    delete p2;

    p1 = marsh.unmarshal(bytes, false).getPortable<TestPortableCycle1>();

    BOOST_REQUIRE(p1);
    BOOST_REQUIRE(p1->p2);

    BOOST_REQUIRE_EQUAL(p1, p1->p2->p1);

    BOOST_REQUIRE_EQUAL(p1->val1, 10);
    BOOST_REQUIRE_EQUAL(p1->p2->val1, 10.5);

    delete p1->p2;
    delete p1;
}

class TestPortableCustom : public GridPortable {
public:
    TestPortableCustom() {
    }

    TestPortableCustom(int f) : flag(f) {
    }

    int32_t typeId() const {
        return 50;
    }

    void writePortable(GridPortableWriter& writer) const {
        if (rawMarshalling) {
            GridPortableRawWriter& raw = writer.rawWriter();

            raw.writeInt32(flag);

            if (flag == 0)
                raw.writeInt32(1);
            else if (flag == 1)
                raw.writeInt64(100);
            else if (flag == 2)
                raw.writeString("string");
            else
                BOOST_FAIL("Invalid flag");
        }
        else {
            writer.writeInt32("flag", flag);

            if (flag == 0)
                writer.writeInt32("0", 1);
            else if (flag == 1)
                writer.writeInt64("1", 100);
            else if (flag == 2)
                writer.writeString("2", "string");
            else
                BOOST_FAIL("Invalid flag");
        }
    }

    void readPortable(GridPortableReader& reader) {
        if (rawMarshalling) {
            GridPortableRawReader& raw = reader.rawReader();

            flag = raw.readInt32();

            if (flag == 0)
                BOOST_REQUIRE_EQUAL(1, raw.readInt32());
            else if (flag == 1)
                BOOST_REQUIRE_EQUAL(100, raw.readInt64());
            else if (flag == 2)
                BOOST_REQUIRE_EQUAL("string", raw.readString().get());
            else
                BOOST_FAIL("Invalid flag");
        }
        else {
            flag = reader.readInt32("flag");

            if (flag == 0)
                BOOST_REQUIRE_EQUAL(1, reader.readInt32("0"));
            else if (flag == 1)
                BOOST_REQUIRE_EQUAL(100, reader.readInt64("1"));
            else if (flag == 2)
                BOOST_REQUIRE_EQUAL("string", reader.readString("2").get());
            else
                BOOST_FAIL("Invalid flag");
        }
    }

    static bool rawMarshalling;

    int32_t flag;
};

bool TestPortableCustom::rawMarshalling = false;

REGISTER_TYPE(TestPortableCustom);

void testCustomSerialization(bool rawMarshalling) {
    TestPortableCustom::rawMarshalling = rawMarshalling;

    GridPortableMarshaller marsh;

    for (int i = 0; i < 3; i++) {
        boost::shared_ptr<std::vector<int8_t>> bytes;

        TestPortableCustom c(i);

        bytes = marsh.marshal(GridClientVariant(&c));

        TestPortableCustom* p = marsh.unmarshal(bytes, false).getPortable<TestPortableCustom>();

        BOOST_REQUIRE_EQUAL(i, p->flag);

        delete p;
    }
}

BOOST_AUTO_TEST_CASE(testPortableSerialization_custom) {
    testCustomSerialization(true);

    testCustomSerialization(true);
}

class TestPortableInvalid : public GridPortable {
public:
    TestPortableInvalid() : invalidWrite(false) {
    }

    TestPortableInvalid(bool invalidWrite, int32_t val1, int32_t val2) : invalidWrite(invalidWrite), val1(val1), val2(val2) {
    }

    int32_t typeId() const {
        return 60;
    }

    void writePortable(GridPortableWriter& writer) const {
        if (invalidWrite) {
            writer.rawWriter().writeInt32(val1);

            writer.writeInt32("named", val2); // Try write named field after raw.
        }
        else {
            writer.writeInt32("named", val2);

            writer.rawWriter().writeInt32(val1);
        }
    }

    void readPortable(GridPortableReader& reader) {
        val1 = reader.rawReader().readInt32();

        val2 = reader.readInt32("named");
    }

    bool invalidWrite;

    int32_t val1;

    int32_t val2;
};

REGISTER_TYPE(TestPortableInvalid);

BOOST_AUTO_TEST_CASE(testPortableSerialization_invalid) {
    GridPortableMarshaller marsh;

    TestPortableInvalid invalid(true, 100, 200);

    try {
        cout << "Try marshal.\n";

        marsh.marshal(GridClientVariant(&invalid));

        BOOST_FAIL("Exception must be thrown");
    }
    catch (GridClientPortableException e) {
        cout << "expected exception " << e.what() << "\n";
    }

    TestPortableInvalid valid(false, 100, 200);

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(GridClientVariant(&valid));

    unique_ptr<TestPortableInvalid> p(marsh.unmarshal(bytes, false).getPortable<TestPortableInvalid>());

    BOOST_REQUIRE_EQUAL(100, (*p).val1);
    BOOST_REQUIRE_EQUAL(200, (*p).val2);
}

class TestPortableFieldNames2 : public GridPortable {
public:
    TestPortableFieldNames2() {
    }

    TestPortableFieldNames2(int32_t f1, int32_t f2, int32_t f3, bool readAllFlag) : f1(f1), f2(f2), f3(f3),
        readAllFlag(readAllFlag) {
    }

    int32_t typeId() const {
        return 71;
    }

    void writePortable(GridPortableWriter& writer) const {
        writer.writeBool("readAllFlag", readAllFlag);
        writer.writeInt32("f1", f1);
        writer.writeInt32("f2", f2);
        writer.writeInt32("f3", f3);

        writer.writeString("f4", "string");
        writer.writeFloat("f5", 10.5f);

        writer.rawWriter().writeInt32(f1);
        writer.rawWriter().writeInt32(f2);
        writer.rawWriter().writeInt32(f3);
    }

    void readPortable(GridPortableReader& reader) {
        f2 = reader.readInt32("f2");
        f1 = reader.readInt32("f1");

        readAllFlag = reader.readBool("readAllFlag");

        BOOST_REQUIRE_EQUAL(0, reader.readInt32("noField1"));

        if (readAllFlag) {
            f3 = reader.readInt32("f3");

            BOOST_REQUIRE_EQUAL(10.5f, reader.readFloat("f5"));
            BOOST_REQUIRE(string("string") == reader.readString("f4"));

            BOOST_REQUIRE_EQUAL(f1, reader.rawReader().readInt32());
            BOOST_REQUIRE_EQUAL(f2, reader.rawReader().readInt32());
            BOOST_REQUIRE_EQUAL(f3, reader.rawReader().readInt32());
        }
        else
            f3 = -1;
    }

    int32_t f1;

    int32_t f2;

    int32_t f3;

    bool readAllFlag;
};

class TestPortableFieldNames1 : public GridPortable {
public:
    TestPortableFieldNames1() : obj1(0), obj2(0), obj3(0), f1(0) {
    }

    TestPortableFieldNames1(int32_t f1) : f1(f1) {
        obj1 = new TestPortableFieldNames2(100, 100, 100, true);
        obj2 = new TestPortableFieldNames2(200, 200, 200, false);
        obj3 = new TestPortableFieldNames2(300, 300, 300, true);
    }

    ~TestPortableFieldNames1() {
        if (obj1)
            delete obj1;
        if (obj2)
            delete obj2;
        if (obj3)
            delete obj3;
    }

    int32_t typeId() const {
        return 70;
    }

    void writePortable(GridPortableWriter& writer) const {
        writer.writeInt32("f1", f1);

        writer.writeVariant("obj1", obj1);
        writer.writeVariant("obj2", obj2);
        writer.writeVariant("obj3", obj3);
    }

    void readPortable(GridPortableReader& reader) {
        obj2 = reader.readVariant("obj2").getPortable<TestPortableFieldNames2>();

        BOOST_REQUIRE_EQUAL(200, obj2->f1);
        BOOST_REQUIRE_EQUAL(200, obj2->f2);
        BOOST_REQUIRE_EQUAL(-1, obj2->f3);

        BOOST_REQUIRE_EQUAL(0, reader.readInt32("f2"));
        BOOST_REQUIRE_EQUAL(0, reader.readInt32("f3"));
        BOOST_REQUIRE_EQUAL(false, reader.readVariant("f5").hasAnyValue());

        f1 = reader.readInt32("f1");

        obj1 = reader.readVariant("obj1").getPortable<TestPortableFieldNames2>();
        obj3 = reader.readVariant("obj3").getPortable<TestPortableFieldNames2>();

        BOOST_REQUIRE_EQUAL(100, obj1->f1);
        BOOST_REQUIRE_EQUAL(100, obj1->f2);
        BOOST_REQUIRE_EQUAL(100, obj1->f3);

        BOOST_REQUIRE_EQUAL(300, obj3->f1);
        BOOST_REQUIRE_EQUAL(300, obj3->f2);
        BOOST_REQUIRE_EQUAL(300, obj3->f3);
    }

    int32_t f1;

    TestPortableFieldNames2* obj1;
    TestPortableFieldNames2* obj2;
    TestPortableFieldNames2* obj3;
};

REGISTER_TYPE(TestPortableFieldNames1);

REGISTER_TYPE(TestPortableFieldNames2);

BOOST_AUTO_TEST_CASE(testPortableSerialization_fieldNames) {
    GridPortableMarshaller marsh;

    TestPortableFieldNames1 obj(1000);

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(GridClientVariant(&obj));

    unique_ptr<TestPortableFieldNames1> p(marsh.unmarshal(bytes, false).getPortable<TestPortableFieldNames1>());

    BOOST_REQUIRE_EQUAL(1000, (*p).f1);
    BOOST_REQUIRE((*p).obj1);
    BOOST_REQUIRE((*p).obj2);
    BOOST_REQUIRE((*p).obj3);
}

void checkVariants(GridClientVariant& var1, GridClientVariant& var2, GridClientVariant& var3) {
    cout << "Variants debugString [var1=" << var1.debugString() << ", var2=" << var2.debugString() << ", var3=" << var3.debugString() << "]\n";

    cout << "Variants toString [var1=" << var1 << ", var2=" << var2 << ", var3=" << var3 << "]\n";

    string str = var1.toString();

    BOOST_REQUIRE(str.size() > 0);

    str = var1.debugString();

    BOOST_REQUIRE(str.size() > 0);

    GridClientVariant nullVar;

    if (var1.hasPortable()) {
        try {
            var1.hashCode();

            BOOST_FAIL("Exception must be thrown");
        }
        catch(runtime_error e) {
            cout << "expected exception " << e.what() << "\n";
        }

        try {
            var1 == var2;

            BOOST_FAIL("Exception must be thrown");
        }
        catch(runtime_error e) {
            cout << "expected exception " << e.what() << "\n";
        }

        var1 = var2;

        BOOST_REQUIRE(var1.hasPortable());

        var1 = std::move(var3);

        BOOST_REQUIRE(var1.hasPortable());

        BOOST_REQUIRE(!var3.hasAnyValue());
        BOOST_REQUIRE(var3 == nullVar);

        return;
    }

    BOOST_REQUIRE(!(var1 == nullVar));

    BOOST_REQUIRE(var1.hasAnyValue());

    BOOST_REQUIRE(var1 == var1);
    BOOST_REQUIRE(!(var1 == var2));
    BOOST_REQUIRE(var1 == var3);
    BOOST_REQUIRE_EQUAL(var1.hashCode(), var3.hashCode());

    var1 = var1;

    BOOST_REQUIRE(var1 == var3);
    BOOST_REQUIRE_EQUAL(var1.hashCode(), var3.hashCode());

    var1 = std::move(var1);

    BOOST_REQUIRE(var1 == var3);
    BOOST_REQUIRE_EQUAL(var1.hashCode(), var3.hashCode());

    GridClientVariant var4(var3);

    BOOST_REQUIRE(var1 == var4);
    BOOST_REQUIRE_EQUAL(var1.hashCode(), var4.hashCode());

    var4 = var1;

    BOOST_REQUIRE(var4 == var3);
    BOOST_REQUIRE_EQUAL(var4.hashCode(), var3.hashCode());

    var3 = std::move(var1);

    BOOST_REQUIRE(!var1.hasAnyValue());
    BOOST_REQUIRE(!(var1 == var3));
    BOOST_REQUIRE(var1 == nullVar);

    BOOST_REQUIRE(var4 == var3);
    BOOST_REQUIRE_EQUAL(var4.hashCode(), var3.hashCode());

    nullVar = var3;

    BOOST_REQUIRE(var4 == nullVar);
    BOOST_REQUIRE_EQUAL(var4.hashCode(), nullVar.hashCode());

    unique_ptr<PortablePerson> p(new PortablePerson(10, "ppppppp"));

    GridClientVariant portVar(p.get());

    BOOST_REQUIRE(!(var4 == portVar));
}

BOOST_AUTO_TEST_CASE(testVariants_allTypes) {
    {
        TGridClientVariantPair val1 = TGridClientVariantPair(1, 1);
        TGridClientVariantPair val2 = TGridClientVariantPair(2, 1);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasMapEntry());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE_EQUAL(val1, var1.getMapEntry());

        checkVariants(var1, var2, var3);
    }

    {
        bool val1 = true;
        bool val2 = false;

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasBool());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE_EQUAL(val1, var1.getBool());

        checkVariants(var1, var2, var3);
    }

    {
        int8_t val1 = 1;
        int8_t val2 = 2;

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasByte());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE_EQUAL(val1, var1.getByte());

        checkVariants(var1, var2, var3);
    }

    {
        int16_t val1 = 1;
        int16_t val2 = 2;

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasShort());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE_EQUAL(val1, var1.getShort());

        checkVariants(var1, var2, var3);
    }

    {
        int32_t val1 = 1;
        int32_t val2 = 2;

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasInt());
        BOOST_REQUIRE(!var1.hasByte());

        BOOST_REQUIRE_EQUAL(val1, var1.getInt());

        checkVariants(var1, var2, var3);
    }

    {
        int64_t val1 = 1;
        int64_t val2 = 2;

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasLong());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE_EQUAL(val1, var1.getLong());

        checkVariants(var1, var2, var3);
    }

    {
        float val1 = 1.5f;
        float val2 = 2.5f;

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasFloat());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE_EQUAL(val1, var1.getFloat());

        checkVariants(var1, var2, var3);
    }

    {
        double val1 = 1;
        double val2 = 2;

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasDouble());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE_EQUAL(val1, var1.getDouble());

        checkVariants(var1, var2, var3);
    }

    {
        uint16_t val1 = 1;
        uint16_t val2 = 2;

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasChar());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE_EQUAL(val1, var1.getChar());

        checkVariants(var1, var2, var3);
    }

    {
        string val1("1");
        string val2("2");

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasString());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getString());

        checkVariants(var1, var2, var3);
    }

    {
        wstring val1(L"1");
        wstring val2(L"2");

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasWideString());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getWideString());

        checkVariants(var1, var2, var3);
    }

    {
        GridClientUuid val1(1, 1);
        GridClientUuid val2(1, 2);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasUuid());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE_EQUAL(val1, var1.getUuid());

        checkVariants(var1, var2, var3);
    }

    {
        GridClientDate val1(1);
        GridClientDate val2(2);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasDate());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE_EQUAL(val1, var1.getDate());

        checkVariants(var1, var2, var3);
    }

    {
        vector<int8_t> val1(1, 1);
        vector<int8_t> val2(1, 2);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasByteArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getByteArray());

        checkVariants(var1, var2, var3);

        GridClientVariant var4(std::move(val1));

        checkVariants(var4, var2, var3);
    }

    {
        vector<int16_t> val1(1, 1);
        vector<int16_t> val2(1, 2);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasShortArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getShortArray());

        checkVariants(var1, var2, var3);

        GridClientVariant var4(std::move(val1));

        checkVariants(var4, var2, var3);
    }

    {
        vector<int32_t> val1(1, 1);
        vector<int32_t> val2(1, 2);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasIntArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getIntArray());

        checkVariants(var1, var2, var3);

        GridClientVariant var4(std::move(val1));

        checkVariants(var4, var2, var3);
    }

    {
        vector<int64_t> val1(1, 1);
        vector<int64_t> val2(1, 2);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasLongArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getLongArray());

        checkVariants(var1, var2, var3);

        GridClientVariant var4(std::move(val1));

        checkVariants(var4, var2, var3);
    }

    {
        vector<float> val1(1, 1.5f);
        vector<float> val2(1, 2.5f);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasFloatArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getFloatArray());

        checkVariants(var1, var2, var3);

        GridClientVariant var4(std::move(val1));

        checkVariants(var4, var2, var3);
    }

    {
        vector<double> val1(1, 1.5);
        vector<double> val2(1, 2.5);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasDoubleArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getDoubleArray());

        checkVariants(var1, var2, var3);

        GridClientVariant var4(std::move(val1));

        checkVariants(var4, var2, var3);
    }

    {
        vector<uint16_t> val1(1, 1);
        vector<uint16_t> val2(1, 2);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasCharArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getCharArray());

        checkVariants(var1, var2, var3);

        GridClientVariant var4(std::move(val1));

        checkVariants(var4, var2, var3);
    }

    {
        vector<bool> val1(1, true);
        vector<bool> val2(1, false);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasBoolArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getBoolArray());

        checkVariants(var1, var2, var3);

        GridClientVariant var4(std::move(val1));

        checkVariants(var4, var2, var3);
    }

    {
        vector<string> val1(1, string("1"));
        vector<string> val2(1, string("2"));

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasStringArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getStringArray());

        checkVariants(var1, var2, var3);

        GridClientVariant var4(std::move(val1));

        checkVariants(var4, var2, var3);
    }

    {
        vector<GridClientUuid> val1(1, GridClientUuid(1, 1));
        vector<GridClientUuid> val2(1, GridClientUuid(1, 2));

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasUuidArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getUuidArray());

        checkVariants(var1, var2, var3);
    }

    {
        vector<GridClientDate> val1(1, GridClientDate(1));
        vector<GridClientDate> val2(1, GridClientDate(2));

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasDateArray());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getDateArray());

        checkVariants(var1, var2, var3);
    }

    {
        TGridClientVariantSet val1(1, GridClientVariant(1));
        TGridClientVariantSet val2(1, GridClientVariant(2));

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasVariantVector());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getVariantVector());

        checkVariants(var1, var2, var3);
    }

    {
        TGridClientVariantMap val1;
        TGridClientVariantMap val2;

        val1[GridClientVariant(1)] = GridClientVariant(1);
        val2[GridClientVariant(1)] = GridClientVariant(2);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasVariantMap());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1 == var1.getVariantMap());

        checkVariants(var1, var2, var3);
    }

    {
        unique_ptr<PortablePerson> val1(new PortablePerson(1, "1"));
        unique_ptr<PortablePerson> val2(new PortablePerson(2, "2"));

        GridClientVariant var1(val1.get());
        GridClientVariant var2(val2.get());
        GridClientVariant var3(val1.get());

        BOOST_REQUIRE(var1.hasHashablePortable());
        BOOST_REQUIRE(!var1.hasPortable());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1.get() == var1.getHashablePortable());

        checkVariants(var1, var2, var3);
    }

    {
        unique_ptr<TestPortable1> val1(new TestPortable1());
        unique_ptr<TestPortable1> val2(new TestPortable1());

        GridClientVariant var1(val1.get());
        GridClientVariant var2(val2.get());
        GridClientVariant var3(val1.get());

        BOOST_REQUIRE(var1.hasPortable());
        BOOST_REQUIRE(!var1.hasHashablePortable());
        BOOST_REQUIRE(!var1.hasInt());

        BOOST_REQUIRE(val1.get() == var1.getPortable());

        checkVariants(var1, var2, var3);
    }
}

BOOST_AUTO_TEST_CASE(testVariantMap) {
    TGridClientVariantMap map;

    TestHashablePortable p1_1(1);
    TestHashablePortable p1_2(2);
    TestHashablePortable p1_3(3);

    TestHashablePortable2 p2_1(1);
    TestHashablePortable2 p2_2(2);
    TestHashablePortable2 p2_3(3);

    GridClientVariant varInt_1(1);
    GridClientVariant varInt_2(2);
    GridClientVariant varInt_3(3);

    GridClientVariant varP1_1(&p1_1);
    GridClientVariant varP1_2(&p1_2);
    GridClientVariant varP1_3(&p1_3);

    GridClientVariant varP2_1(&p2_1);
    GridClientVariant varP2_2(&p2_2);
    GridClientVariant varP2_3(&p2_3);

    map[varInt_1] = 1;
    map[varP1_1] = 2;
    map[varP2_1] = 3;

    map[varInt_2] = 4;
    map[varP1_2] = 5;
    map[varP2_2] = 6;

    map[varInt_3] = 7;
    map[varP1_3] = 8;
    map[varP2_3] = 9;

    BOOST_REQUIRE_EQUAL(map[varInt_1], 1);
    BOOST_REQUIRE_EQUAL(map[varP1_1], 2);
    BOOST_REQUIRE_EQUAL(map[varP2_1], 3);

    BOOST_REQUIRE_EQUAL(map[varInt_2], 4);
    BOOST_REQUIRE_EQUAL(map[varP1_2], 5);
    BOOST_REQUIRE_EQUAL(map[varP2_2], 6);

    BOOST_REQUIRE_EQUAL(map[varInt_3], 7);
    BOOST_REQUIRE_EQUAL(map[varP1_3], 8);
    BOOST_REQUIRE_EQUAL(map[varP2_3], 9);
}

BOOST_AUTO_TEST_CASE(testVariant) {
    TestNonHashablePortable nh1;
    TestNonHashablePortable nh2;

    TestHashablePortable h1(1);
    TestHashablePortable h2(2);
    TestHashablePortable h3(1);

    GridClientVariant nhVar1(&nh1);
    GridClientVariant nhVar2(&nh2);
    GridClientVariant hVar1(&h1);
    GridClientVariant hVar2(&h2);
    GridClientVariant hVar3(&h3);

    BOOST_REQUIRE_EQUAL(true, nhVar1.hasPortable());
    BOOST_REQUIRE_EQUAL(false, nhVar1.hasHashablePortable());

    BOOST_REQUIRE_EQUAL(true, nhVar2.hasPortable());
    BOOST_REQUIRE_EQUAL(false, nhVar2.hasHashablePortable());

    BOOST_REQUIRE_EQUAL(false, hVar1.hasPortable());
    BOOST_REQUIRE_EQUAL(true, hVar1.hasHashablePortable());

    BOOST_REQUIRE_EQUAL(false, hVar2.hasPortable());
    BOOST_REQUIRE_EQUAL(true, hVar2.hasHashablePortable());

    BOOST_REQUIRE_EQUAL(false, hVar3.hasPortable());
    BOOST_REQUIRE_EQUAL(true, hVar3.hasHashablePortable());

    try {
        cout << "Try get hashCode " << nhVar1.hashCode();

        BOOST_FAIL("Exception must be thrown");
    }
    catch (exception e) {
        cout << "expected exception " << e.what() << "\n";
    }

    BOOST_REQUIRE_EQUAL(1, hVar1.hashCode());

    BOOST_REQUIRE_EQUAL(2, hVar2.hashCode());

    BOOST_REQUIRE_EQUAL(1, hVar3.hashCode());

    try {
        cout << "Try compare " << (nhVar1 == nhVar2);

        BOOST_FAIL("Exception must be thrown");
    }
    catch (exception e) {
        cout << "expected exception " << e.what() << "\n";
    }

    BOOST_REQUIRE_EQUAL(false, hVar1 == hVar2);

    BOOST_REQUIRE_EQUAL(true, hVar1 == hVar3);
}

BOOST_AUTO_TEST_CASE(testPortableSerialization) {
    GridPortableMarshaller marsh;

    PortablePerson p(-10, "ABC");

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(GridClientVariant(&p));

    PortablePerson* pRead = marsh.unmarshal(bytes, false).getPortable<PortablePerson>();

    cout << "Unmarshalled " << pRead->getId() << " " << pRead->getName() << "\n";

    BOOST_CHECK_EQUAL(-10, pRead->getId());
    BOOST_CHECK_EQUAL("ABC", pRead->getName());

    delete pRead;

    GridClientVariant var = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE(var.hasPortableObject());

    GridPortableObject& portable = var.getPortableObject();

    BOOST_CHECK_EQUAL(-10, portable.field("id").getInt());
    BOOST_CHECK_EQUAL("ABC", portable.field("name").getString());

    GridClientVariant nullField = portable.field("invalidName");

    BOOST_REQUIRE(!nullField.hasAnyValue());
}

BOOST_AUTO_TEST_CASE(testMarshal_byte) {
    int8_t val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE_EQUAL(10, varRead.getByte());
}

BOOST_AUTO_TEST_CASE(testMarshal_bool) {
    bool val = true;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE_EQUAL(true, varRead.getBool());
}

BOOST_AUTO_TEST_CASE(testMarshal_char) {
    uint16_t val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE_EQUAL(10, varRead.getChar());
}

BOOST_AUTO_TEST_CASE(testMarshal_chort) {
    int16_t val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE_EQUAL(10, varRead.getShort());
}

BOOST_AUTO_TEST_CASE(testMarshal_int) {
    int32_t val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE_EQUAL(10, varRead.getInt());
}

BOOST_AUTO_TEST_CASE(testMarshal_long) {
    int64_t val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE_EQUAL(10, varRead.getLong());
}

BOOST_AUTO_TEST_CASE(testMarshal_float) {
    float val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE_EQUAL(10, varRead.getFloat());
}

BOOST_AUTO_TEST_CASE(testMarshal_double) {
    double val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE_EQUAL(10, varRead.getDouble());
}

BOOST_AUTO_TEST_CASE(testMarshal_str) {
    string val("str");

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE_EQUAL("str", varRead.getString());
}

BOOST_AUTO_TEST_CASE(testMarshal_uuid) {
    GridClientUuid val(10, 20);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE(GridClientUuid(10, 20) == varRead.getUuid());
}

BOOST_AUTO_TEST_CASE(testMarshal_date) {
    GridClientDate val(10);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    BOOST_REQUIRE(GridClientDate(10) == varRead.getDate());
}

BOOST_AUTO_TEST_CASE(testMarshal_byteArr) {
    vector<int8_t> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(i);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<int8_t>& valRead = varRead.getByteArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE_EQUAL(i, valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_boolArr) {
    vector<bool> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(i % 2 == 0);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<bool>& valRead = varRead.getBoolArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE_EQUAL(i % 2 == 0, valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_int16Arr) {
    vector<int16_t> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(i);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<int16_t>& valRead = varRead.getShortArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE_EQUAL(i, valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_charArr) {
    vector<uint16_t> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(i);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<uint16_t>& valRead = varRead.getCharArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE_EQUAL(i, valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_int32Arr) {
    vector<int32_t> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(i);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<int32_t>& valRead = varRead.getIntArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE_EQUAL(i, valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_int64Arr) {
    vector<int64_t> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(i);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<int64_t>& valRead = varRead.getLongArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE_EQUAL(i, valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_floatArr) {
    vector<float> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(i);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<float>& valRead = varRead.getFloatArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE_EQUAL(i, valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_doubleArr) {
    vector<double> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(i);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<double>& valRead = varRead.getDoubleArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE_EQUAL(i, valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_stringArr) {
    vector<string> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(i % 2 == 0 ? "0" : "1");

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<string>& valRead = varRead.getStringArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE_EQUAL(i % 2 == 0 ? "0" : "1", valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_uuidArr) {
    vector<GridClientUuid> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(GridClientUuid(i, i));

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<GridClientUuid>& valRead = varRead.getUuidArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE(GridClientUuid(i, i) == valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_dateArr) {
    vector<GridClientDate> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(GridClientDate(i));

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<GridClientDate> valRead = varRead.getDateArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE(GridClientDate(i) == valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_variantArr) {
    vector<GridClientVariant> val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(GridClientVariant(i));

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    vector<GridClientVariant>& valRead = varRead.getVariantVector();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE_EQUAL(i, valRead[i].getInt());
}

BOOST_AUTO_TEST_CASE(testMarshal_variantMap) {
    TGridClientVariantMap val;

    int size = 3;

    for (int i = 0; i < size; i++)
        val[GridClientVariant(i)] = GridClientVariant(i + 0.5);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes, true);

    TGridClientVariantMap& valRead = varRead.getVariantMap();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++) {
        GridClientVariant& mapVal = valRead[GridClientVariant(i)];

        BOOST_REQUIRE_EQUAL(i + 0.5, mapVal.getDouble());
    }
}

class TestNested3 : public GridPortable {
public:
    TestNested3() {
    }

    int32_t typeId() const {
        return 82;
    }

    void writePortable(GridPortableWriter& writer) const {
        writer.writeDouble("f1", 2.5);

        writer.rawWriter().writeString("str");
    }

    void readPortable(GridPortableReader& reader) {
        val = reader.readDouble("f1");

        string rawVal = reader.rawReader().readString().get();

        BOOST_REQUIRE_EQUAL("str", rawVal);
    }

    double val;
};

class TestNested2 : public GridPortable {
public:
    TestNested2() {
    }

    int32_t typeId() const {
        return 81;
    }

    void writePortable(GridPortableWriter& writer) const {
        writer.writeInt32("f1", 1);

        writer.writeVariant("f3", new TestNested3());

        writer.writeBool("f2", true);

        writer.rawWriter().writeInt32(20);
    }

    void readPortable(GridPortableReader& reader) {
        val = reader.readInt32("f1");
        flag = reader.readBool("f2");

        reader.readVariant("f3");

        int32_t raw = reader.rawReader().readInt32();

        BOOST_REQUIRE_EQUAL(20, raw);
    }

    int32_t val;

    bool flag;
};

class TestNested1 : public GridPortable {
public:
    TestNested1() : obj(0) {
    }

    TestNested1(TestNested2* obj) : obj(obj) {
    }

    ~TestNested1() {
        if (obj)
            delete obj;
    }

    int32_t typeId() const {
        return 80;
    }

    void writePortable(GridPortableWriter& writer) const {
        writer.writeVariant("1", obj);

        writer.writeFloat("2", 1.5f);

        writer.rawWriter().writeInt32(10);
    }

    void readPortable(GridPortableReader& reader) {
        GridClientVariant var = reader.readVariant("1");

        obj = var.getPortable<TestNested2>();

        BOOST_REQUIRE_EQUAL(1, obj->val);

        vFloat = reader.readFloat("2");

        BOOST_REQUIRE_EQUAL(10, reader.rawReader().readInt32());
    }

    TestNested2* obj;

    float vFloat;
};

REGISTER_TYPE(TestNested1);
REGISTER_TYPE(TestNested2);
REGISTER_TYPE(TestNested3);

BOOST_AUTO_TEST_CASE(testMarshal_nested) {
    GridPortableMarshaller marsh;

    TestNested1 obj(new TestNested2());

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(GridClientVariant(&obj));

    GridClientVariant var1 = marsh.unmarshal(bytes, false);

    TestNested1* obj1 = var1.getPortable<TestNested1>();

    BOOST_REQUIRE_EQUAL(1, obj1->obj->val);
    BOOST_REQUIRE_EQUAL(1.5, obj1->vFloat);

    delete obj1;

    GridPortableObject port1 = marsh.unmarshal(bytes, true).getPortableObject();

    GridClientVariant f1 = port1.field("1");

    GridClientVariant f2 = port1.field("2");

    GridClientVariant invalid = port1.field("invalid");

    BOOST_REQUIRE_EQUAL(1.5f, f2.getFloat());

    BOOST_REQUIRE(f1.hasPortableObject());

    BOOST_REQUIRE(!invalid.hasAnyValue());

    GridPortableObject port2 = f1.getPortableObject();

    GridClientVariant f11 = port2.field("f1");

    BOOST_REQUIRE_EQUAL(1, f11.getInt());

    GridClientVariant f12 = port2.field("f2");

    BOOST_REQUIRE(f12.getBool());

    invalid = port2.field("invalid");

    BOOST_REQUIRE(!invalid.hasAnyValue());

    GridClientVariant f3 = port2.field("f3");

    BOOST_REQUIRE(f3.hasPortableObject());

    GridPortableObject port3 = f3.getPortableObject();

    GridClientVariant f21 = port3.field("f1");

    BOOST_REQUIRE_EQUAL(2.5, f21.getDouble());

    TestNested3* obj3 = port3.deserialize<TestNested3>();

    BOOST_REQUIRE_EQUAL(2.5, obj3->val);

    delete obj3;
}

class TestObjCollection : public GridPortable {
public:
    static bool rawMarshalling;

    int32_t typeId() const {
        return 90;
    }

    void writePortable(GridPortableWriter& writer) const {
        if (rawMarshalling) {
            writer.rawWriter().writeInt32(1);

            writer.rawWriter().writeVariantCollection(col);

            writer.rawWriter().writeInt32(3);
        }
        else {
            writer.writeInt32("1", 1);

            writer.writeVariantCollection("2", col);

            writer.writeInt32("3", 3);
        }
    }

    void readPortable(GridPortableReader& reader) {
        if (rawMarshalling) {
            val1 = reader.rawReader().readInt32();

            reader.rawReader().readVariantCollection(col);

            val2 = reader.rawReader().readInt32();
        }
        else {
            val1 = reader.readInt32("1");

            reader.readVariantCollection("2", col);

            val2 = reader.readInt32("3");
        }
    }

    int32_t val1;

    vector<GridClientVariant> col;

    int32_t val2;
};

bool TestObjCollection::rawMarshalling = false;

void testObjCollectionMarshal(bool raw)  {
    TestObjCollection::rawMarshalling = raw;

    TestObjCollection obj;

    PortablePerson p1(1, "n1");
    PortablePerson p2(2, "n2");

    vector<GridClientVariant> col;

    col.push_back(&p1);
    col.push_back(&p2);
    col.push_back(&p1);

    obj.col = col;

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(GridClientVariant(&obj));

    GridClientVariant var = marsh.unmarshal(bytes, true);

    GridPortableObject& port = var.getPortableObject();

    TestObjCollection* objRead = port.deserialize<TestObjCollection>();

    BOOST_REQUIRE_EQUAL(1, objRead->val1);
    BOOST_REQUIRE_EQUAL(3, objRead->val2);

    BOOST_REQUIRE_EQUAL(3, objRead->col.size());

    GridClientVariant p1Var = objRead->col[0];

    PortablePerson* p1Read = p1Var.getPortableObject().deserialize<PortablePerson>();

    BOOST_REQUIRE_EQUAL(1, p1Read->getId());
    BOOST_REQUIRE_EQUAL("n1", p1Read->getName());

    GridClientVariant p2Var = objRead->col[1];

    PortablePerson* p2Read = p2Var.getPortableObject().deserialize<PortablePerson>();

    BOOST_REQUIRE_EQUAL(2, p2Read->getId());
    BOOST_REQUIRE_EQUAL("n2", p2Read->getName());

    GridClientVariant p3Var = objRead->col[2];

    PortablePerson* p3Read = p3Var.getPortableObject().deserialize<PortablePerson>();

    BOOST_REQUIRE(p1Read == p3Read);

    delete p1Read;
    delete p2Read;
    delete objRead;
}

REGISTER_TYPE(TestObjCollection);

BOOST_AUTO_TEST_CASE(testObjCollection) {
    testObjCollectionMarshal(false);

    testObjCollectionMarshal(true);
}

BOOST_AUTO_TEST_CASE(testPortableObject) {
    std::unique_ptr<PortableAddress> addr(new PortableAddress("addr1"));
    
    PortablePerson p1(1, "n1", addr.get());
    PortablePerson p2(2, "n2");

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshal(GridClientVariant(&p1));

    GridPortableObject o1 = marsh.unmarshal(bytes, true).getPortableObject();
    
    BOOST_REQUIRE_EQUAL(1, o1.field("id").getInt());
    BOOST_REQUIRE_EQUAL("n1", o1.field("name").getString());
    BOOST_REQUIRE(!o1.field("invalid").hasAnyValue());

    GridPortableObject addr1 = o1.field("addr").getPortableObject();

    BOOST_REQUIRE_EQUAL("addr1", addr1.field("addr").getString());
    BOOST_REQUIRE(!addr1.field("invalid").hasAnyValue());

    bytes = marsh.marshal(GridClientVariant(&p2));

    GridPortableObject o2 = marsh.unmarshal(bytes, true).getPortableObject();

    BOOST_REQUIRE_EQUAL(2, o2.field("id").getInt());
    BOOST_REQUIRE_EQUAL("n2", o2.field("name").getString());

    BOOST_REQUIRE_EQUAL(p1.hashCode(), o1.hashCode());
    BOOST_REQUIRE_EQUAL(p1.typeId(), p1.typeId());

    BOOST_REQUIRE_EQUAL(p2.hashCode(), o2.hashCode());
    BOOST_REQUIRE_EQUAL(p2.typeId(), o2.typeId());

    BOOST_REQUIRE(!(o1 == o2));

    GridPortableObject o3 = marsh.unmarshal(bytes, true).getPortableObject();

    BOOST_REQUIRE_EQUAL(2, o3.field("id").getInt());

    BOOST_REQUIRE(o2 == o3);
}

BOOST_AUTO_TEST_CASE(testCacheRequestMarshal) {
    std::unique_ptr<PortableAddress> addr1(new PortableAddress("addr1"));
    std::unique_ptr<PortableAddress> addr2(new PortableAddress("addr2"));
    
    PortablePerson p1(1, "n1", addr1.get());
    PortablePerson p2(2, "n2", addr1.get());
    PortablePerson p3(3, "n3");
    PortablePerson p4(4, "n4", addr2.get());

    string cacheName("cacheName");

    GridCacheRequestCommand cmd(GridCacheRequestCommand::PUT_ALL, cacheName);

    GridClientVariant key(&p1);
    GridClientVariant value(&p2);

    cmd.setKey(key);
    cmd.setValue(value);
    cmd.setValue2(value);

    TGridClientVariantMap vals;

    vals[GridClientVariant(&p1)] = GridClientVariant(&p2);
    vals[GridClientVariant(&p3)] = GridClientVariant(&p4);

    cmd.setValues(vals);

    GridClientCacheRequest msg(cmd);

    GridPortableMarshaller marsh;

    boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshalSystemObject(msg);

    BOOST_REQUIRE_EQUAL(631, bytes->size());
}

class TestTemplateApiReadWrite : public GridPortable {
public:
    TestTemplateApiReadWrite() {
    }

    TestTemplateApiReadWrite(int colSize) {
        for (int i = 0; i < colSize; i++) {
            
        }
    }

    int32_t typeId() const {
        return 100;
    }

    void writePortable(GridPortableWriter& writer) const {
        writer.writeByteArray("_bArr", bArr.begin(), bArr.end());
        writer.writeInt16Array("_sArr", sArr.begin(), sArr.end());
        writer.writeInt32Array("_iArr", iArr.begin(), iArr.end());
        writer.writeInt64Array("_lArr", lArr.begin(), lArr.end());
        writer.writeFloatArray("_fArr", fArr.begin(), fArr.end());
        writer.writeDoubleArray("_dArr", dArr.begin(), dArr.end());
        writer.writeCharArray("_cArr", cArr.begin(), cArr.end());
        writer.writeBoolArray("_boolArr", boolArr.begin(), boolArr.end());
        writer.writeStringArray("_strArr", strArr.begin(), strArr.end());
        writer.writeUuidArray("_uuidArr", uuidArr.begin(), uuidArr.end());
        writer.writeDateArray("_dateArr", dateArr.begin(), dateArr.end());
        
        writer.writeVariantCollection("_col", GridCollectionType::LINKED_LIST, col.begin(), col.end());
        writer.writeVariantCollection("_varDeq", GridCollectionType::LINKED_LIST, varDeq.begin(), varDeq.end());
        writer.writeVariantCollection("_varList", GridCollectionType::LINKED_LIST, varList.begin(), varList.end());

        writer.writeInt64Array("_lDeq", lDeq.begin(), lDeq.end());

        GridPortableRawWriter& raw = writer.rawWriter();

        raw.writeByteArray(bArrRaw.begin(), bArrRaw.end());
        raw.writeInt16Array(sArrRaw.begin(), sArrRaw.end());
        raw.writeInt32Array(iArrRaw.begin(), iArrRaw.end());
        raw.writeInt64Array(lArrRaw.begin(), lArrRaw.end());
        raw.writeFloatArray(fArrRaw.begin(), fArrRaw.end());
        raw.writeDoubleArray(dArrRaw.begin(), dArrRaw.end());
        raw.writeCharArray(cArrRaw.begin(), cArrRaw.end());
        raw.writeBoolArray(boolArrRaw.begin(), boolArrRaw.end());
        raw.writeStringArray(strArrRaw.begin(), strArrRaw.end());
        raw.writeUuidArray(uuidArrRaw.begin(), uuidArrRaw.end());
        raw.writeDateArray(dateArrRaw.begin(), dateArrRaw.end());
        raw.writeVariantArray(objArrRaw.begin(), objArrRaw.end());
        
        raw.writeVariantCollection(GridCollectionType::ARRAY_LIST, colRaw.begin(), colRaw.end());
        raw.writeVariantCollection(GridCollectionType::LINKED_LIST, varDeq.begin(), varDeq.end());
        raw.writeVariantCollection(GridCollectionType::LINKED_LIST, varList.begin(), varList.end());
        
        raw.writeInt64Array(lDeqRaw.begin(), lDeqRaw.end());
        raw.writeStringArray(strSetRaw.begin(), strSetRaw.end());
    }            
    
    void readPortable(GridPortableReader& reader) {
        reader.readByteArray("_bArr", back_insert_iterator<vector<int8_t>>(bArr));
        reader.readInt16Array("_sArr", back_insert_iterator<vector<int16_t>>(sArr));
        reader.readInt32Array("_iArr", back_insert_iterator<vector<int32_t>>(iArr));
        reader.readInt64Array("_lArr", back_insert_iterator<vector<int64_t>>(lArr));
        reader.readFloatArray("_fArr", back_insert_iterator<vector<float>>(fArr));
        reader.readDoubleArray("_dArr", back_insert_iterator<vector<double>>(dArr));
        reader.readCharArray("_cArr", back_insert_iterator<vector<uint16_t>>(cArr));
        reader.readBoolArray("_boolArr", back_insert_iterator<vector<bool>>(boolArr));
        reader.readStringArray("_strArr", back_insert_iterator<vector<string>>(strArr));
        reader.readWStringArray("_wstrArr", back_insert_iterator<vector<wstring>>(wstrArr));
        reader.readUuidArray("_uuidArr", back_insert_iterator<vector<GridClientUuid>>(uuidArr));
        reader.readDateArray("_dateArr", back_insert_iterator<vector<GridClientDate>>(dateArr));

        reader.readVariantCollection("col", back_insert_iterator<vector<GridClientVariant>>(col));
        reader.readVariantCollection("_varDeq", back_insert_iterator<deque<GridClientVariant>>(varDeq));
        reader.readVariantCollection("_varList", back_insert_iterator<list<GridClientVariant>>(varList));

        reader.readInt64Array("lDeq", back_insert_iterator<deque<int64_t>>(lDeq));
        reader.readStringArray("strSet", insert_iterator<set<string>>(strSet, strSet.begin()));
        reader.readWStringArray("wstrSet", insert_iterator<set<wstring>>(wstrSet, wstrSet.begin()));
        reader.readUuidArray("uuidList", back_insert_iterator<list<GridClientUuid>>(uuidList));
        reader.readDateArray("dateList", back_insert_iterator<list<GridClientDate>>(dateList));

        GridPortableRawReader& raw = reader.rawReader();

        raw.readByteArray(back_insert_iterator<vector<int8_t>>(bArrRaw));
        raw.readInt16Array(back_insert_iterator<vector<int16_t>>(sArrRaw));
        raw.readInt32Array(back_insert_iterator<vector<int32_t>>(iArrRaw));
        raw.readInt64Array(back_insert_iterator<vector<int64_t>>(lArrRaw));
        raw.readFloatArray(back_insert_iterator<vector<float>>(fArrRaw));
        raw.readDoubleArray(back_insert_iterator<vector<double>>(dArrRaw));
        raw.readCharArray(back_insert_iterator<vector<uint16_t>>(cArrRaw));
        raw.readBoolArray(back_insert_iterator<vector<bool>>(boolArrRaw));
        raw.readStringArray(back_insert_iterator<vector<string>>(strArrRaw));
        raw.readUuidArray(back_insert_iterator<vector<GridClientUuid>>(uuidArrRaw));
        raw.readDateArray(back_insert_iterator<vector<GridClientDate>>(dateArrRaw));

        raw.readVariantCollection(back_insert_iterator<vector<GridClientVariant>>(colRaw));
        raw.readVariantCollection(back_insert_iterator<deque<GridClientVariant>>(varDeqRaw));
        raw.readVariantCollection(back_insert_iterator<list<GridClientVariant>>(varListRaw));

        raw.readInt64Array(back_insert_iterator<deque<int64_t>>(lDeqRaw));
        raw.readStringArray(insert_iterator<set<string>>(strSetRaw, strSetRaw.begin()));
        raw.readWStringArray(insert_iterator<set<wstring>>(wstrSetRaw, wstrSetRaw.begin()));
        raw.readUuidArray(back_insert_iterator<list<GridClientUuid>>(uuidListRaw));
        raw.readDateArray(back_insert_iterator<list<GridClientDate>>(dateListRaw));
    }
    
    /** */
    deque<int64_t> lDeq;

    /** */
    deque<int64_t> lDeqRaw;

    /** */
    set<string> strSet;

    /** */
    set<string> strSetRaw;

    /** */
    set<wstring> wstrSet;

    /** */
    set<wstring> wstrSetRaw;

    /** */
    list<GridClientUuid> uuidList;

    /** */
    list<GridClientUuid> uuidListRaw;

    /** */
    list<GridClientDate> dateList;

    /** */
    list<GridClientDate> dateListRaw;

    /** */
    deque<GridClientVariant> varDeq;

    /** */
    deque<GridClientVariant> varDeqRaw;

    /** */
    list<GridClientVariant> varList;

    /** */
    list<GridClientVariant> varListRaw;

    /** */
    vector<int8_t> bArr;

    /** */
    vector<int8_t> bArrRaw;

    /** */
    vector<int16_t> sArr;

    /** */
    vector<int16_t> sArrRaw;

    /** */
    vector<int32_t> iArr;

    /** */
    vector<int32_t> iArrRaw;

    /** */
    vector<int64_t> lArr;

    /** */
    vector<int64_t> lArrRaw;

    /** */
    vector<float> fArr;

    /** */
    vector<float> fArrRaw;

    /** */
    vector<double> dArr;

    /** */
    vector<double> dArrRaw;

    /** */
    vector<uint16_t> cArr;

    /** */
    vector<uint16_t> cArrRaw;

    /** */
    vector<bool> boolArr;

    /** */
    vector<bool> boolArrRaw;

    /** */
    vector<string> strArr;

    /** */
    vector<wstring> wstrArr;

    /** */
    vector<string> strArrRaw;

    /** */
    vector<string> wstrArrRaw;

    /** */
    vector<GridClientUuid> uuidArr;

    /** */
    vector<GridClientUuid> uuidArrRaw;

    /** */
    vector<GridClientDate> dateArr;

    /** */
    vector<GridClientDate> dateArrRaw;

    /** */
    TGridClientVariantSet objArr;

    /** */
    TGridClientVariantSet objArrRaw;

    /** */
    TGridClientVariantSet col;

    /** */
    TGridClientVariantSet colRaw;
};

REGISTER_TYPE(TestTemplateApiReadWrite);

BOOST_AUTO_TEST_CASE(testTemplateApi) {
    TestTemplateApiReadWrite obj(10);    
}

BOOST_AUTO_TEST_SUITE_END()
