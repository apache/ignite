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

#include <boost/shared_ptr.hpp>
#include <boost/test/unit_test.hpp>

#include <gridgain/gridgain.hpp>

#include "gridgain/gridclientvariant.hpp"
#include "gridgain/gridportableserializer.hpp"

#include "gridgain/impl/gridclienttopology.hpp"
#include "gridgain/impl/gridclientdataprojection.hpp"
#include "gridgain/impl/gridclientpartitionedaffinity.hpp"
#include "gridgain/impl/gridclientshareddata.hpp"
#include "gridgain/impl/cmd/gridclienttcpcommandexecutor.hpp"
#include "gridgain/impl/connection/gridclientconnectionpool.hpp"
#include "gridgain/impl/hash/gridclientvarianthasheableobject.hpp"
#include "gridgain/impl/hash/gridclientsimpletypehasheableobject.hpp"
#include "gridgain/impl/hash/gridclientstringhasheableobject.hpp"
#include "gridgain/impl/hash/gridclientfloathasheableobject.hpp"
#include "gridgain/impl/hash/gridclientdoublehasheableobject.hpp"
#include "gridgain/impl/marshaller/gridnodemarshallerhelper.hpp"
#include "gridgain/gridclientfactory.hpp"

#include "gridgain/impl/marshaller/portable/gridportablemarshaller.hpp"

#include <boost/unordered_map.hpp>

using namespace std;

BOOST_AUTO_TEST_SUITE(GridClientPortableSuite)
/*
GridClientConfiguration clientConfig() {
    GridClientConfiguration clientConfig;

    vector<GridClientSocketAddress> servers;

    servers.push_back(GridClientSocketAddress("127.0.0.1", 11212));

    clientConfig.servers(servers);

    GridClientProtocolConfiguration protoCfg;

    clientConfig.protocolConfiguration(protoCfg);

	vector<GridClientDataConfiguration> dataCfgVec;

	GridClientDataConfiguration dataCfg;
    GridClientPartitionAffinity* aff = new GridClientPartitionAffinity();

    dataCfg.name("partitioned");
    dataCfg.affinity(shared_ptr<GridClientDataAffinity>(aff));

    dataCfgVec.push_back(dataCfg);

	clientConfig.dataConfiguration(dataCfgVec);

    return clientConfig;
}

class TestNonHashablePortable : public GridPortable {
public:
    int32_t typeId() const override {
        return 0;
    }

    void writePortable(GridPortableWriter& writer) const override {
        BOOST_FAIL("Should not be called");
    }

    void readPortable(GridPortableReader& reader) override {
        BOOST_FAIL("Should not be called");
    }
};

class TestHashablePortable : public GridHashablePortable {
public:
    TestHashablePortable() {
    }

    TestHashablePortable(int32_t pId) : id(pId) {
    }

    int32_t typeId() const override {
        return 1000;
    }

    void writePortable(GridPortableWriter& writer) const override {
        BOOST_FAIL("Should not be called");
    }

    void readPortable(GridPortableReader& reader) override {
        BOOST_FAIL("Should not be called");
    }
    
    int hashCode() const override {
        return id;
    }

    bool operator==(const GridHashablePortable& other) const override {
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

    int32_t typeId() const override {
        return 1001;
    }

    void writePortable(GridPortableWriter& writer) const override {
        BOOST_FAIL("Should not be called");
    }

    void readPortable(GridPortableReader& reader) override {
        BOOST_FAIL("Should not be called");
    }
    
    int hashCode() const override {
        return id;
    }

    bool operator==(const GridHashablePortable& other) const override {
        return id == static_cast<const TestHashablePortable*>(&other)->id;
    }

    int32_t id;
};

class PortablePerson : public GridHashablePortable {
public:
    PortablePerson() {
    }

    PortablePerson(int id, string name) : name(name), id(id) {
    }

    int32_t getId() {
        return id;
    }

    string getName() {
        return name;
    }

	int32_t typeId() const {
		return 100;
	}

    void writePortable(GridPortableWriter &writer) const override {
        writer.writeString("name", name);
		writer.writeInt32("id", id);
	}

    void readPortable(GridPortableReader &reader) override {
		name = reader.readString("name").get_value_or(std::string());
        id = reader.readInt32("id");
	}

    bool operator==(const GridHashablePortable& portable) const override {
        const PortablePerson* other = static_cast<const PortablePerson*>(&portable);

        return id == other->id;
    }

    int hashCode() const override {
        return id;
    }

private:
    string name;

    int32_t id;
};

REGISTER_TYPE(100, PortablePerson);

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

class PersonSerializer : public GridPortableSerializer<Person> {
public:
    void writePortable(Person* obj, GridPortableWriter& writer) override {
        writer.writeInt32("id", obj->getId());
        writer.writeString("name", obj->getName());
    }

    Person* readPortable(GridPortableReader& reader) override {
        int32_t id = reader.readInt32("id");
        string name = reader.readString("name").get_value_or(std::string());

        return new Person(id, name);
    }

    int32_t typeId(Person* obj) override {
        return 101;
    }

    int32_t hashCode(Person* obj) override {
        return obj->getId();
    }

    bool compare(Person* obj1, Person* obj2) override {
        return obj1->getId() == obj2->getId();
    }
};

REGISTER_TYPE_SERIALIZER(101, Person, PersonSerializer);

class TestPortable1 : public GridPortable {
public:
    TestPortable1() : arraysSize(-1) {
    }

    void writePortable(GridPortableWriter &writer) const {
        if (rawMarshalling) {
            GridPortableRawWriter& raw = writer.rawWriter();

            raw.writeBool(vBool);
            raw.writeBoolArray(vBoolArray, arraysSize);
            raw.writeBoolCollection(vBoolVector);

            raw.writeByte(vByte);
            raw.writeByteArray(vByteArray, arraysSize);
            raw.writeByteCollection(vByteVector);

            raw.writeInt16(vInt16);
            raw.writeInt16Array(vInt16Array, arraysSize);
            raw.writeInt16Collection(vInt16Vector);

            raw.writeInt32(vInt32);
            raw.writeInt32Array(vInt32Array, arraysSize);
            raw.writeInt32Collection(vInt32Vector);

            raw.writeInt64(vInt64);
            raw.writeInt64Array(vInt64Array, arraysSize);
            raw.writeInt64Collection(vInt64Vector);

            raw.writeFloat(vFloat);
            raw.writeFloatArray(vFloatArray, arraysSize);
            raw.writeFloatCollection(vFloatVector);

            raw.writeDouble(vDouble);
            raw.writeDoubleArray(vDoubleArray, arraysSize);
            raw.writeDoubleCollection(vDoubleVector);

            raw.writeString(vStr);
            raw.writeStringCollection(vStrVector);

            raw.writeWString(vWStr);
            raw.writeWStringCollection(vWStrVector);

            raw.writeVariant(vVariant);
            raw.writeVariantCollection(vVariantVector);
            raw.writeVariantMap(vVariantMap);
        }
        else {
            writer.writeBool("1", vBool);
            writer.writeBoolArray("2", vBoolArray, arraysSize);
            writer.writeBoolCollection("3", vBoolVector);

            writer.writeByte("4", vByte);
            writer.writeByteArray("5", vByteArray, arraysSize);
            writer.writeByteCollection("6", vByteVector);

            writer.writeInt16("7", vInt16);
            writer.writeInt16Array("8", vInt16Array, arraysSize);
            writer.writeInt16Collection("9", vInt16Vector);

            writer.writeInt32("10", vInt32);
            writer.writeInt32Array("11", vInt32Array, arraysSize);
            writer.writeInt32Collection("12", vInt32Vector);

            writer.writeInt64("13", vInt64);
            writer.writeInt64Array("14", vInt64Array, arraysSize);
            writer.writeInt64Collection("15", vInt64Vector);

            writer.writeFloat("16", vFloat);
            writer.writeFloatArray("17", vFloatArray, arraysSize);
            writer.writeFloatCollection("18", vFloatVector);

            writer.writeDouble("19", vDouble);
            writer.writeDoubleArray("20", vDoubleArray, arraysSize);
            writer.writeDoubleCollection("21", vDoubleVector);

            writer.writeString("22", vStr);
            writer.writeStringCollection("23", vStrVector);

            writer.writeWString("24", vWStr);
            writer.writeWStringCollection("25", vWStrVector);

            writer.writeVariant("26", vVariant);
            writer.writeVariantCollection("27", vVariantVector);
            writer.writeVariantMap("28", vVariantMap);
        }
	}

    void readPortable(GridPortableReader &reader) {
        BOOST_REQUIRE_EQUAL(-1, arraysSize);

        if (rawMarshalling) {
            GridPortableRawReader& raw = reader.rawReader();

            vBool = raw.readBool();
            pair<bool*, int32_t> boolArr = raw.readBoolArray();
            arraysSize = boolArr.second;
            vBoolVector = raw.readBoolCollection().get();

            vByte = raw.readByte();
            pair<int8_t*, int32_t> byteArr = raw.readByteArray();
            BOOST_REQUIRE_EQUAL(arraysSize, byteArr.second);
            vByteVector = raw.readByteCollection().get();

            vInt16 = raw.readInt16();
            pair<int16_t*, int32_t> int16Arr = raw.readInt16Array();
            BOOST_REQUIRE_EQUAL(arraysSize, int16Arr.second);
            vInt16Vector = raw.readInt16Collection().get();

            vInt32 = raw.readInt32();
            pair<int32_t*, int32_t> int32Arr = raw.readInt32Array();
            BOOST_REQUIRE_EQUAL(arraysSize, int32Arr.second);
            vInt32Vector = raw.readInt32Collection().get();

            vInt64 = raw.readInt64();
            pair<int64_t*, int32_t> int64Arr = raw.readInt64Array();
            BOOST_REQUIRE_EQUAL(arraysSize, int64Arr.second);
            vInt64Vector = raw.readInt64Collection().get();

            vFloat = raw.readFloat();
            pair<float*, int32_t> floatArr = raw.readFloatArray();
            BOOST_REQUIRE_EQUAL(arraysSize, floatArr.second);
            vFloatVector = raw.readFloatCollection().get();

            vDouble = raw.readDouble();
            pair<double*, int32_t> doubleArr = raw.readDoubleArray();
            BOOST_REQUIRE_EQUAL(arraysSize, doubleArr.second);
            vDoubleVector = raw.readDoubleCollection().get();

            vStr = raw.readString().get();
            vStrVector = raw.readStringCollection().get();

            vWStr = raw.readWString().get();
            vWStrVector = raw.readWStringCollection().get();

            vVariant = raw.readVariant();
            vVariantVector = raw.readVariantCollection().get();
            vVariantMap = raw.readVariantMap().get();
        }
        else {
            vBool = reader.readBool("1");
            pair<bool*, int32_t> boolArr = reader.readBoolArray("2");
            vBoolVector = reader.readBoolCollection("3").get();

            arraysSize = boolArr.second;

            vByte = reader.readByte("4");
            pair<int8_t*, int32_t> byteArr = reader.readByteArray("5");
            BOOST_REQUIRE_EQUAL(arraysSize, byteArr.second);
            vByteVector = reader.readByteCollection("6").get();

            vInt16 = reader.readInt16("7");
            pair<int16_t*, int32_t> int16Arr = reader.readInt16Array("8");
            BOOST_REQUIRE_EQUAL(arraysSize, int16Arr.second);
            vInt16Vector = reader.readInt16Collection("9").get();

            vInt32 = reader.readInt32("10");
            pair<int32_t*, int32_t> int32Arr = reader.readInt32Array("11");
            BOOST_REQUIRE_EQUAL(arraysSize, int32Arr.second);
            vInt32Vector = reader.readInt32Collection("12").get();

            vInt64 = reader.readInt64("13");
            pair<int64_t*, int32_t> int64Arr = reader.readInt64Array("14");
            BOOST_REQUIRE_EQUAL(arraysSize, int64Arr.second);
            vInt64Vector = reader.readInt64Collection("15").get();

            vFloat = reader.readFloat("16");
            pair<float*, int32_t> floatArr = reader.readFloatArray("17");
            BOOST_REQUIRE_EQUAL(arraysSize, floatArr.second);
            vFloatVector = reader.readFloatCollection("18").get();

            vDouble = reader.readDouble("19");
            pair<double*, int32_t> doubleArr = reader.readDoubleArray("20");
            BOOST_REQUIRE_EQUAL(arraysSize, doubleArr.second);
            vDoubleVector = reader.readDoubleCollection("21").get();

            vStr = reader.readString("22").get();
            vStrVector = reader.readStringCollection("23").get();

            vWStr = reader.readWString("24").get();
            vWStrVector = reader.readWStringCollection("25").get();

            vVariant = reader.readVariant("26");
            vVariantVector = reader.readVariantCollection("27").get();
            vVariantMap = reader.readVariantMap("28").get();
        }
	}

    int32_t typeId() const override {
        return 300;
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

    GridClientVariant vVariant;

    vector<GridClientVariant> vVariantVector;

    TGridClientVariantMap vVariantMap;
};

bool TestPortable1::rawMarshalling = false;

REGISTER_TYPE(300, TestPortable1);

TestPortable1 createTestPortable1(int32_t arraysSize) {
    TestPortable1 p;

    p.arraysSize = arraysSize;

    p.vBool = true;
    p.vBoolArray = new bool[arraysSize];
    for (int i = 0; i < arraysSize; i++)
        p.vByteArray[i] = i % 2 == 0;
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

    p.vVariant = GridClientVariant(1);
    p.vVariantVector = vector<GridClientVariant>(arraysSize, GridClientVariant(2));

    p.vVariantMap = TGridClientVariantMap();

    for (int i = 0; i < arraysSize; i++)
        p.vVariantMap[GridClientVariant(i)] = GridClientVariant(i);

    return p;
}

void validateTestPortable1(TestPortable1 p, int32_t arraysSize) {
    BOOST_REQUIRE_EQUAL(arraysSize, p.arraysSize);

    BOOST_REQUIRE_EQUAL(true, p.vBool);
    for (int i = 0; i < arraysSize; i++) {
        BOOST_REQUIRE_EQUAL(i % 2, p.vByteArray[i]);
        BOOST_REQUIRE_EQUAL(true, p.vBoolVector[i]);
    }

    BOOST_REQUIRE_EQUAL(1, p.vByte);
    for (int i = 0; i < arraysSize; i++) {
        BOOST_REQUIRE_EQUAL(i % 2 == 0 ? 1 : -1, p.vByteArray[i]);
        BOOST_REQUIRE_EQUAL(-1, p.vByteVector[i]);
    }

    BOOST_REQUIRE_EQUAL(1, p.vInt16);
    for (int i = 0; i < arraysSize; i++) {
        BOOST_REQUIRE_EQUAL(i % 2 == 0 ? 1 : -1, p.vInt16Array[i]);
        BOOST_REQUIRE_EQUAL(-1, p.vInt16Vector[i]);
    }

    BOOST_REQUIRE_EQUAL(1, p.vInt32);
    for (int i = 0; i < arraysSize; i++) {
        BOOST_REQUIRE_EQUAL(i % 2 == 0 ? 1 : -1, p.vInt32Array[i]);
        BOOST_REQUIRE_EQUAL(-1, p.vInt32Vector[i]);
    }

    BOOST_REQUIRE_EQUAL(1, p.vInt64);
    for (int i = 0; i < arraysSize; i++) {
        BOOST_REQUIRE_EQUAL(i % 2 == 0 ? 1 : -1, p.vInt64Array[i]);
        BOOST_REQUIRE_EQUAL(-1, p.vInt64Vector[i]);
    }

    BOOST_REQUIRE_EQUAL(1.5, p.vFloat);
    for (int i = 0; i < arraysSize; i++) {
        BOOST_REQUIRE_EQUAL(i % 2 == 0 ? 1.5 : -1.5, p.vFloatArray[i]);
        BOOST_REQUIRE_EQUAL(-1, p.vFloatVector[i]);
    }

    BOOST_REQUIRE_EQUAL(1.5, p.vDouble);
    for (int i = 0; i < arraysSize; i++) {
        BOOST_REQUIRE_EQUAL(i % 2 == 0 ? 1.5 : -1.5, p.vDoubleArray[i]);
        BOOST_REQUIRE_EQUAL(-1, p.vDoubleVector[i]);
    }

    BOOST_REQUIRE_EQUAL("str1", p.vStr);

    BOOST_REQUIRE_EQUAL(arraysSize, p.vStrVector.size());

    for (int i = 0; i < arraysSize; i++)
        BOOST_REQUIRE_EQUAL("str2", p.vStrVector[i]);

    BOOST_REQUIRE(L"wstr1" == p.vWStr);

    BOOST_REQUIRE_EQUAL(arraysSize, p.vWStrVector.size());

    for (int i = 0; i < arraysSize; i++)
        BOOST_REQUIRE(L"wstr2" == p.vWStrVector[i]);

    BOOST_REQUIRE_EQUAL(1, p.vUuid.mostSignificantBits());
    BOOST_REQUIRE_EQUAL(2, p.vUuid.leastSignificantBits());

    BOOST_REQUIRE_EQUAL(arraysSize, p.vUuidVector.size());

    for (int i = 0; i < arraysSize; i++) {
        BOOST_REQUIRE_EQUAL(1, p.vUuidVector[i].mostSignificantBits());
        BOOST_REQUIRE_EQUAL(2, p.vUuidVector[i].leastSignificantBits());
    }

    BOOST_REQUIRE_EQUAL(true, p.vVariant.hasInt());

    BOOST_REQUIRE_EQUAL(1, p.vVariant.getInt());

    BOOST_REQUIRE_EQUAL(arraysSize, p.vVariantVector.size());

    for (int i = 0; i < arraysSize; i++) {
        BOOST_REQUIRE_EQUAL(true, p.vVariantVector[i].hasInt());

        BOOST_REQUIRE_EQUAL(2, p.vVariantVector[i].getInt());
    }

    BOOST_REQUIRE_EQUAL(arraysSize, p.vVariantMap.size());
    
    for (int i = 0; i < arraysSize; i++) {
        GridClientVariant val = p.vVariantMap[GridClientVariant(i)];

        BOOST_REQUIRE_EQUAL(true, val.hasInt());
        BOOST_REQUIRE_EQUAL(i, val.getInt());
    }
}

void testTestPortable1Marshalling(bool rawMarshalling, int32_t arraysSize) {
    GridPortableMarshaller marsh;

    TestPortable1 p = createTestPortable1(arraysSize);

    TestPortable1::rawMarshalling = rawMarshalling;

    vector<int8_t> bytes = marsh.marshal(p);

    TestPortable1* pRead = marsh.unmarshalUserObject<TestPortable1>(bytes);

    validateTestPortable1(*pRead, arraysSize);

    delete pRead;
}

BOOST_AUTO_TEST_CASE(testPortableSerialization_allTypes) {
    testTestPortable1Marshalling(false, 10);
    testTestPortable1Marshalling(false, 10000);
    testTestPortable1Marshalling(false, 0);

    testTestPortable1Marshalling(true, 10);
    testTestPortable1Marshalling(true, 10000);
    testTestPortable1Marshalling(true, 0);
}

class TestPortableCycle2;

class TestPortableCycle1 : public GridPortable {
public:
    int32_t typeId() const override {
        return 400;
    }

    void writePortable(GridPortableWriter& writer) const override {
        writer.writeInt32("1", val1);
        writer.writeVariant("2", GridClientVariant(p2));
    }

    void readPortable(GridPortableReader& reader) override {
        val1 = reader.readInt32("1");
        p2 = reader.readVariant("2").getPortable<TestPortableCycle2>();
    }

    TestPortableCycle2* p2;

    int32_t val1;
};

REGISTER_TYPE(400, TestPortableCycle1);

class TestPortableCycle2 : public GridPortable {
public:
    int32_t typeId() const override {
        return 401;
    }

    void writePortable(GridPortableWriter& writer) const override {
        writer.writeFloat("1", val1);
        writer.writeVariant("2", GridClientVariant(p1));
    }

    void readPortable(GridPortableReader& reader) override {
        val1 = reader.readFloat("1");
        p1 = reader.readVariant("2").getPortable<TestPortableCycle1>();
    }

    TestPortableCycle1* p1;

    float val1;
};

REGISTER_TYPE(401, TestPortableCycle2);

BOOST_AUTO_TEST_CASE(testPortableSerialization_cycle) {
    GridPortableMarshaller marsh;

    TestPortableCycle1* p1 = new TestPortableCycle1();
    
    p1->val1 = 10;

    TestPortableCycle2* p2 = new TestPortableCycle2();

    p2->val1 = 10.5;

    p1->p2 = p2;
    p2->p1 = p1;

    vector<int8_t> bytes = marsh.marshal(*p1);

    delete p1;
    delete p2;

    p1 = marsh.unmarshalUserObject<TestPortableCycle1>(bytes);

    BOOST_REQUIRE(p1 != nullptr);
    BOOST_REQUIRE(p1->p2 != nullptr);
    BOOST_REQUIRE(p2->p1 != nullptr);

    BOOST_REQUIRE_EQUAL(p1, p1->p2->p1);

    BOOST_REQUIRE_EQUAL(p1->val1, 10);
    BOOST_REQUIRE_EQUAL(p2->val1, 10.5);

    delete p1->p2;
    delete p1;
}

class TestPortableCustom : public GridPortable {
public:
    TestPortableCustom() {
    }

    TestPortableCustom(int f) : flag(f) {
    }
    
    int32_t typeId() const override {
        return 500;
    }

    void writePortable(GridPortableWriter& writer) const override {
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

    void readPortable(GridPortableReader& reader) override {
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
                BOOST_REQUIRE_EQUAL(1, reader.readInt32("1"));
            else if (flag == 1)
                BOOST_REQUIRE_EQUAL(100, reader.readInt64("2"));
            else if (flag == 2)
                BOOST_REQUIRE_EQUAL("string", reader.readString("3").get());
            else
                BOOST_FAIL("Invalid flag");
        }
    }
    
    static bool rawMarshalling;

    int32_t flag;
};

bool TestPortableCustom::rawMarshalling = false;

REGISTER_TYPE(500, TestPortableCustom);

void testCustomSerialization(bool rawMarshalling) {
    TestPortableCustom::rawMarshalling = rawMarshalling;

    GridPortableMarshaller marsh;

    for (int i = 0; i < 3; i++) {
        vector<int8_t> bytes;
    
        TestPortableCustom c(i);

        bytes = marsh.marshal(c);

        TestPortableCustom* p = marsh.unmarshalUserObject<TestPortableCustom>(bytes);

        BOOST_REQUIRE_EQUAL(i, p->flag);

        delete p;
    }
}

BOOST_AUTO_TEST_CASE(testPortableSerialization_custom) {
    testCustomSerialization(false);

    testCustomSerialization(true);
}

class TestPortableInvalid : public GridPortable {
public:
    TestPortableInvalid() : invalidWrite(false) {
    }
    
    TestPortableInvalid(bool invalidWrite, int32_t val1, int32_t val2) : invalidWrite(invalidWrite), val1(val1), val2(val2) {
    }
    
    int32_t typeId() const override {
        return 600;
    }

    void writePortable(GridPortableWriter& writer) const override {
        if (invalidWrite) {
            writer.rawWriter().writeInt32(val1);

            writer.writeInt32("named", val2); // Try write named field after raw.
        }
        else {
            writer.writeInt32("named", val2);

            writer.rawWriter().writeInt32(val1);
        }
    }

    void readPortable(GridPortableReader& reader) override {
        val1 = reader.rawReader().readInt32();

        val2 = reader.readInt32("named");
    }

    bool invalidWrite;

    int32_t val1;

    int32_t val2;
};

REGISTER_TYPE(600, TestPortableInvalid);

BOOST_AUTO_TEST_CASE(testPortableSerialization_invalid) {
    GridPortableMarshaller marsh;

    TestPortableInvalid invalid(true, 100, 200);

    try {
        cout << "Try marshal.\n";

        marsh.marshal(invalid);

        BOOST_FAIL("Exception must be thrown");
    }
    catch (GridClientPortableException e) {
        cout << "expected exception " << e.what() << "\n";
    }

    TestPortableInvalid valid(false, 100, 200);

    vector<int8_t> bytes = marsh.marshal(valid);

    unique_ptr<TestPortableInvalid> p(marsh.unmarshalUserObject<TestPortableInvalid>(bytes));
    
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

    int32_t typeId() const override {
        return 701;
    }

    void writePortable(GridPortableWriter& writer) const override {
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

    void readPortable(GridPortableReader& reader) override {
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
    TestPortableFieldNames1() : obj1(nullptr), obj2(nullptr), obj3(nullptr), f1(0) {
    }

    TestPortableFieldNames1(int32_t f1) : f1(f1) {  
        obj1 = new TestPortableFieldNames2(100, 100, 100, true);
        obj2 = new TestPortableFieldNames2(200, 200, 200, false); 
        obj3 = new TestPortableFieldNames2(300, 300, 300, true);
    }

    ~TestPortableFieldNames1() {
        if (obj1 != nullptr)
            delete obj1;
        if (obj2 != nullptr)
            delete obj2;
        if (obj3 != nullptr)
            delete obj3;
    }

    int32_t typeId() const override {
        return 700;
    }

    void writePortable(GridPortableWriter& writer) const override {
        writer.writeInt32("f1", 10);
        
        writer.writeVariant("obj1", obj1);
        writer.writeVariant("obj2", obj2);
        writer.writeVariant("obj3", obj3);
    }

    void readPortable(GridPortableReader& reader) override {
        obj2 = reader.readVariant("obj2").getPortable<TestPortableFieldNames2>();

        BOOST_REQUIRE_EQUAL(200, obj2->f1);
        BOOST_REQUIRE_EQUAL(200, obj2->f2);
        BOOST_REQUIRE_EQUAL(-1, obj2->f3);

        BOOST_REQUIRE_EQUAL(0, reader.readInt32("f2"));
        BOOST_REQUIRE_EQUAL(0, reader.readInt32("f3"));
        BOOST_REQUIRE_EQUAL(false, reader.readVariant("f5").hasAnyValue());

        f1 = reader.readInt32("f1");

        obj1 = reader.readVariant("obj1").getPortable<TestPortableFieldNames2>();
        obj2 = reader.readVariant("obj3").getPortable<TestPortableFieldNames2>();

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

REGISTER_TYPE(700, TestPortableFieldNames1);

REGISTER_TYPE(701, TestPortableFieldNames2);

BOOST_AUTO_TEST_CASE(testPortableSerialization_fieldNames) {
    GridPortableMarshaller marsh;

    TestPortableFieldNames1 obj(1000);

    vector<int8_t> bytes = marsh.marshal(obj);

    unique_ptr<TestPortableFieldNames1> p(marsh.unmarshalUserObject<TestPortableFieldNames1>(bytes));
    
    BOOST_REQUIRE_EQUAL(1000, (*p).f1);
    BOOST_REQUIRE((*p).obj1 != nullptr);
    BOOST_REQUIRE((*p).obj2 != nullptr);
    BOOST_REQUIRE((*p).obj3 != nullptr);
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

BOOST_AUTO_TEST_CASE(testVariants) {
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
        vector<int8_t> val1(1, 1);
        vector<int8_t> val2(1, 2);

        GridClientVariant var1(val1);
        GridClientVariant var2(val2);
        GridClientVariant var3(val1);

        BOOST_REQUIRE(var1.hasByteArray());
        BOOST_REQUIRE(!var1.hasInt());
        
        BOOST_REQUIRE(val1 == var1.getByteArray());
        
        checkVariants(var1, var2, var3);
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

    BOOST_REQUIRE_EQUAL(true, hVar1.hasPortable());
    BOOST_REQUIRE_EQUAL(true, hVar1.hasHashablePortable());

    BOOST_REQUIRE_EQUAL(true, hVar2.hasPortable());
    BOOST_REQUIRE_EQUAL(true, hVar2.hasHashablePortable());

    BOOST_REQUIRE_EQUAL(true, hVar3.hasPortable());
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

BOOST_AUTO_TEST_CASE(testWriteHandleTable) {
    GridWriteHandleTable table(3, 3);

    int32_t vals[100];

    for (int i = 0; i < 100; i++) {
        int32_t handle = table.lookup(&vals[i]);

        BOOST_REQUIRE_EQUAL(-1, handle);

        handle = table.lookup(&vals[i]);

        BOOST_REQUIRE_EQUAL(i, handle);
    }

    for (int i = 0; i < 100; i++) {
        int32_t handle = table.lookup(&vals[i]);

        BOOST_REQUIRE_EQUAL(i, handle);
    }
}

BOOST_AUTO_TEST_CASE(testReadHandleTable) {
    GridReadHandleTable table(3);

    int32_t vals[100];

    for (int i = 0; i < 100; i++) {
        int32_t handle = table.assign(&vals[i]);

        BOOST_REQUIRE_EQUAL(i, handle);

        void* obj = table.lookup(i);

        BOOST_REQUIRE_EQUAL(&vals[i], obj);
    }

    for (int i = 0; i < 100; i++) {
        void* obj = table.lookup(i);

        BOOST_REQUIRE_EQUAL(&vals[i], obj);
    }
}

BOOST_AUTO_TEST_CASE(testPortableTask) {
    GridClientConfiguration cfg = clientConfig();

	TGridClientPtr client = GridClientFactory::start(cfg);

    TGridClientComputePtr compute = client->compute();

    PortablePerson person(10, "person1");

    GridClientVariant res = compute->execute("org.gridgain.client.integration.GridClientTcpPortableSelfTest$TestPortableTask", &person);

    BOOST_REQUIRE(res.hasPortable());

    PortablePerson* p = res.getPortable<PortablePerson>();

    BOOST_CHECK_EQUAL(11, p->getId());
    BOOST_CHECK_EQUAL("result", p->getName());

    delete p;
}

BOOST_AUTO_TEST_CASE(testPortableKey) {
    GridClientConfiguration cfg = clientConfig();

	TGridClientPtr client = GridClientFactory::start(cfg);

    TGridClientDataPtr data = client->data("partitioned");

    PortablePerson person1(10, "person1");
    PortablePerson person2(20, "person2");
    PortablePerson person3(30, "person3");

    bool put = data->put(&person1, 100);

    BOOST_CHECK_EQUAL(true, put);

    put = data->put(&person2, 200);

    BOOST_CHECK_EQUAL(true, put);

    GridClientVariant val = data->get(&person1);

    BOOST_REQUIRE(val.hasInt());

    BOOST_CHECK_EQUAL(100, val.getInt());

    val = data->get(&person2);

    BOOST_REQUIRE(val.hasInt());

    BOOST_CHECK_EQUAL(200, val.getInt());

    val = data->get(&person3);

    BOOST_REQUIRE(!val.hasAnyValue());
}

BOOST_AUTO_TEST_CASE(testPortableCache) {
    GridClientConfiguration cfg = clientConfig();

	TGridClientPtr client = GridClientFactory::start(cfg);

    TGridClientDataPtr data = client->data("partitioned");

    PortablePerson person(10, "person1");

    bool put = data->put(100, &person);

    BOOST_CHECK_EQUAL(true, put);

    PortablePerson* p = data->get(100).getPortable<PortablePerson>();

    cout << "Get person: " << p->getId() << " " << p->getName() << "\n";

    BOOST_CHECK_EQUAL(10, p->getId());
    BOOST_CHECK_EQUAL("person1", p->getName());

    delete p;
}

BOOST_AUTO_TEST_CASE(testPortableCachePutAllGetAll) {
    GridClientConfiguration cfg = clientConfig();

	TGridClientPtr client = GridClientFactory::start(cfg);

    TGridClientDataPtr data = client->data("partitioned");

    PortablePerson p1(100, "personAll1");
    PortablePerson p2(200, "personAll2");
    PortablePerson p3(300, "personAll3");

    TGridClientVariantMap map;

    map[GridClientVariant(&p1)] = GridClientVariant(&p1);
    map[GridClientVariant(&p2)] = GridClientVariant(&p2);
    map[GridClientVariant(&p3)] = GridClientVariant(&p3);

    bool put = data->putAll(map);

    BOOST_CHECK_EQUAL(true, put);

    PortablePerson p4(400, "personAll4");

    TGridClientVariantSet keys;

    keys.push_back(GridClientVariant(&p1));
    keys.push_back(GridClientVariant(&p2));
    keys.push_back(GridClientVariant(&p3));
    keys.push_back(GridClientVariant(&p4));

    TGridClientVariantMap getMap = data->getAll(keys);

    BOOST_CHECK_EQUAL(3, getMap.size());
    
    GridClientVariant var;
    
    var = getMap[keys[0]];
    BOOST_CHECK_EQUAL(true, var.hasPortable());
    BOOST_CHECK_EQUAL("personAll1", var.getPortable<PortablePerson>()->getName());
    delete var.getPortable();

    var = getMap[keys[1]];
    BOOST_CHECK_EQUAL(true, var.hasPortable());
    BOOST_CHECK_EQUAL("personAll2", var.getPortable<PortablePerson>()->getName());
    delete var.getPortable();

    var = getMap[keys[2]];
    BOOST_CHECK_EQUAL(true, var.hasPortable());
    BOOST_CHECK_EQUAL("personAll3", var.getPortable<PortablePerson>()->getName());
    delete var.getPortable();

    var = getMap[keys[3]];
    BOOST_CHECK_EQUAL(false, var.hasAnyValue());
}

BOOST_AUTO_TEST_CASE(testExternalPortableCache) {
    GridClientConfiguration cfg = clientConfig();

	TGridClientPtr client = GridClientFactory::start(cfg);

    TGridClientDataPtr data = client->data("partitioned");

    PersonSerializer serializer;

    Person person(20, "person2");

    data->put(200, &GridExternalPortable<Person>(&person, serializer));

    GridExternalPortable<Person>* p = data->get(200).getPortable<GridExternalPortable<Person>>();

    cout << "Get person: " << (*p)->getId() << " " << (*p)->getName() << "\n";

    BOOST_CHECK_EQUAL(20, (*p)->getId());
    BOOST_CHECK_EQUAL("person2", (*p)->getName());

    delete p->getObject();
    delete p;
}

BOOST_AUTO_TEST_CASE(testPortableSerialization) {
    GridPortableMarshaller marsh;

    PortablePerson p(-10, "ABC");

    vector<int8_t> bytes = marsh.marshal(p);

    PortablePerson* pRead = marsh.unmarshalUserObject<PortablePerson>(bytes);

    cout << "Unmarshalled " << pRead->getId() << " " << pRead->getName() << "\n";

    BOOST_CHECK_EQUAL(-10, pRead->getId());
    BOOST_CHECK_EQUAL("ABC", pRead->getName());

    delete pRead;

    GridClientVariant var = marsh.unmarshal(bytes);

    BOOST_REQUIRE(var.hasPortableObject());

    GridPortableObject& portable = var.getPortableObject();

    BOOST_CHECK_EQUAL(-10, portable.field("id").getInt());
    BOOST_CHECK_EQUAL("ABC", portable.field("name").getString());

    GridClientVariant nullField = portable.field("invalidName");

    BOOST_REQUIRE(!nullField.hasAnyValue());
}

BOOST_AUTO_TEST_CASE(testExternalSerialization) {
    GridPortableMarshaller marsh;

    Person person(20, "abc");

    PersonSerializer ser;

    GridExternalPortable<Person> ext(&person, ser);

    vector<int8_t> bytes = marsh.marshal(ext);

    GridExternalPortable<Person>* pRead = marsh.unmarshalUserObject<GridExternalPortable<Person>>(bytes);

    cout << "Unmarshalled " << (*pRead)->getId() << " " << (*pRead)->getName() << "\n";

    BOOST_CHECK_EQUAL(20, (*pRead)->getId());
    BOOST_CHECK_EQUAL("abc", (*pRead)->getName());

    delete pRead->getObject();
    delete pRead;
}
*/
BOOST_AUTO_TEST_CASE(testMarshal_byte) {
    int8_t val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    BOOST_REQUIRE_EQUAL(10, varRead.getByte());
}

BOOST_AUTO_TEST_CASE(testMarshal_bool) {
    bool val = true;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    BOOST_REQUIRE_EQUAL(true, varRead.getBool());
}

BOOST_AUTO_TEST_CASE(testMarshal_char) {
    uint16_t val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    BOOST_REQUIRE_EQUAL(10, varRead.getChar());
}

BOOST_AUTO_TEST_CASE(testMarshal_chort) {
    int16_t val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    BOOST_REQUIRE_EQUAL(10, varRead.getShort());
}

BOOST_AUTO_TEST_CASE(testMarshal_int) {
    int32_t val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    BOOST_REQUIRE_EQUAL(10, varRead.getInt());
}

BOOST_AUTO_TEST_CASE(testMarshal_long) {
    int64_t val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    BOOST_REQUIRE_EQUAL(10, varRead.getLong());
}

BOOST_AUTO_TEST_CASE(testMarshal_float) {
    float val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    BOOST_REQUIRE_EQUAL(10, varRead.getFloat());
}

BOOST_AUTO_TEST_CASE(testMarshal_double) {
    double val = 10;

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    BOOST_REQUIRE_EQUAL(10, varRead.getDouble());
}

BOOST_AUTO_TEST_CASE(testMarshal_str) {
    string val("str");

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    BOOST_REQUIRE_EQUAL("str", varRead.getString());
}

BOOST_AUTO_TEST_CASE(testMarshal_uuid) {
    GridClientUuid val(10, 20);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    BOOST_REQUIRE(GridClientUuid(10, 20) == varRead.getUuid());
}

BOOST_AUTO_TEST_CASE(testMarshal_byteArr) {
    vector<int8_t> val;
    
    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(i);

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

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
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

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
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

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
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

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
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

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
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

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
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

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
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

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
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

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
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    vector<GridClientUuid>& valRead = varRead.getUuidArray();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++)
        BOOST_REQUIRE(GridClientUuid(i, i) == valRead[i]);
}

BOOST_AUTO_TEST_CASE(testMarshal_variantArr) {
    vector<GridClientVariant> val;
    
    int size = 3;

    for (int i = 0; i < size; i++)
        val.push_back(GridClientVariant(i));

    GridClientVariant var(val);

    GridPortableMarshaller marsh;
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

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
    
    vector<int8_t> bytes = marsh.marshal(var);

    GridClientVariant varRead = marsh.unmarshal(bytes);

    TGridClientVariantMap& valRead = varRead.getVariantMap();

    BOOST_REQUIRE_EQUAL(size, valRead.size());

    for (int i = 0; i < size; i++) {
        GridClientVariant& mapVal = valRead[GridClientVariant(i)];

        BOOST_REQUIRE_EQUAL(i + 0.5, mapVal.getDouble());
    }
}

BOOST_AUTO_TEST_SUITE_END()
