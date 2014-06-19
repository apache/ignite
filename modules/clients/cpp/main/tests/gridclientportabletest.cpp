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

class TestPortable : public GridPortable {
public:
    TestPortable() : arraysSize(-1) {
    }

    void writePortable(GridPortableWriter &writer) const {
        if (rawMarshaling) {
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

        if (rawMarshaling) {
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

    static bool rawMarshaling;

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

    boost::unordered_map<GridClientVariant, GridClientVariant> vVariantMap;
};

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
        return 0;
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
