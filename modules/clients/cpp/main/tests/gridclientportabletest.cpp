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
            writer.writeBool(vBool);
            writer.writeBoolArray(vBoolArray, arraysSize);
            writer.writeBoolCollection(vBoolVector);

            writer.writeByte(vByte);
            writer.writeByteArray(vByteArray, arraysSize);
            writer.writeByteCollection(vByteVector);

            writer.writeInt16(vInt16);
            writer.writeInt16Array(vInt16Array, arraysSize);
            writer.writeInt16Collection(vInt16Vector);

            writer.writeInt32(vInt32);
            writer.writeInt32Array(vInt32Array, arraysSize);
            writer.writeInt32Collection(vInt32Vector);

            writer.writeInt64(vInt64);
            writer.writeInt64Array(vInt64Array, arraysSize);
            writer.writeInt64Collection(vInt64Vector);

            writer.writeFloat(vFloat);
            writer.writeFloatArray(vFloatArray, arraysSize);
            writer.writeFloatCollection(vFloatVector);

            writer.writeDouble(vDouble);
            writer.writeDoubleArray(vDoubleArray, arraysSize);
            writer.writeDoubleCollection(vDoubleVector);

            writer.writeString(vStr);
            writer.writeStringCollection(vStrVector);

            writer.writeWString(vWStr);
            writer.writeWStringCollection(vWStrVector);

            writer.writeVariant(vVariant);
            writer.writeVariantCollection(vVariantVector);
            writer.writeVariantMap(vVariantMap);
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
            vBool = reader.readBool();
            pair<bool*, int32_t> boolArr = reader.readBoolArray();
            arraysSize = boolArr.second;
            vBoolVector = reader.readBoolCollection().get();

            vByte = reader.readByte();
            pair<int8_t*, int32_t> byteArr = reader.readByteArray();
            BOOST_REQUIRE_EQUAL(arraysSize, byteArr.second);
            vByteVector = reader.readByteCollection().get();

            vInt16 = reader.readInt16();
            pair<int16_t*, int32_t> int16Arr = reader.readInt16Array();
            BOOST_REQUIRE_EQUAL(arraysSize, int16Arr.second);
            vInt16Vector = reader.readInt16Collection().get();

            vInt32 = reader.readInt32();
            pair<int32_t*, int32_t> int32Arr = reader.readInt32Array();
            BOOST_REQUIRE_EQUAL(arraysSize, int32Arr.second);
            vInt32Vector = reader.readInt32Collection().get();

            vInt64 = reader.readInt64();
            pair<int64_t*, int32_t> int64Arr = reader.readInt64Array();
            BOOST_REQUIRE_EQUAL(arraysSize, int64Arr.second);
            vInt64Vector = reader.readInt64Collection().get();

            vFloat = reader.readFloat();
            pair<float*, int32_t> floatArr = reader.readFloatArray();
            BOOST_REQUIRE_EQUAL(arraysSize, floatArr.second);
            vFloatVector = reader.readFloatCollection().get();

            vDouble = reader.readDouble();
            pair<double*, int32_t> doubleArr = reader.readDoubleArray();
            BOOST_REQUIRE_EQUAL(arraysSize, doubleArr.second);
            vDoubleVector = reader.readDoubleCollection().get();

            vStr = reader.readString().get();
            vStrVector = reader.readStringCollection().get();

            vWStr = reader.readWString().get();
            vWStrVector = reader.readWStringCollection().get();

            vVariant = reader.readVariant();
            vVariantVector = reader.readVariantCollection().get();
            vVariantMap = reader.readVariantMap().get();
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
        p.vFloatArray[i] = i % 2 == 0 ? 1.5 : -1.5;            
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

    TestPortable1* pRead = marsh.unmarshal<TestPortable1>(bytes);

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
        p2 = reader.readVariant().getPortable<TestPortableCycle2>();
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
        p1 = reader.readVariant().getPortable<TestPortableCycle1>();
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

    p1 = marsh.unmarshal<TestPortableCycle1>(bytes);

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
            writer.writeInt32(flag);

            if (flag == 0)
                writer.writeInt32(1);
            else if (flag == 1)
                writer.writeInt64(100);
            else if (flag == 2)
                writer.writeString("string");
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
            flag = reader.readInt32();

            if (flag == 0)
                BOOST_REQUIRE_EQUAL(1, reader.readInt32());
            else if (flag == 1)
                BOOST_REQUIRE_EQUAL(100, reader.readInt64());
            else if (flag == 2)
                BOOST_REQUIRE_EQUAL("string", reader.readString().get());
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

        TestPortableCustom* p = marsh.unmarshal<TestPortableCustom>(bytes);

        BOOST_REQUIRE_EQUAL(i, p->flag);

        delete p;
    }
}

BOOST_AUTO_TEST_CASE(testPortableSerialization_custom) {
    testCustomSerialization(false);

    testCustomSerialization(true);
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

    PortablePerson* pRead = marsh.unmarshal<PortablePerson>(bytes);

    cout << "Unmarshalled " << pRead->getId() << " " << pRead->getName() << "\n";

    BOOST_CHECK_EQUAL(-10, pRead->getId());
    BOOST_CHECK_EQUAL("ABC", pRead->getName());

    delete pRead;
}

BOOST_AUTO_TEST_CASE(testExternalSerialization) {
    GridPortableMarshaller marsh;

    Person person(20, "abc");

    PersonSerializer ser;

    GridExternalPortable<Person> ext(&person, ser);

    vector<int8_t> bytes = marsh.marshal(ext);

    GridExternalPortable<Person>* pRead = marsh.unmarshal<GridExternalPortable<Person>>(bytes);

    cout << "Unmarshalled " << (*pRead)->getId() << " " << (*pRead)->getName() << "\n";

    BOOST_CHECK_EQUAL(20, (*pRead)->getId());
    BOOST_CHECK_EQUAL("abc", (*pRead)->getName());

    delete pRead->getObject();
    delete pRead;
}

BOOST_AUTO_TEST_SUITE_END()
