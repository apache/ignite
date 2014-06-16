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

#include <unordered_map>

using namespace std;

BOOST_AUTO_TEST_SUITE(GridClientPortableSuite)

GridClientConfiguration clientConfig() {
    GridClientConfiguration clientConfig;

    vector<GridClientSocketAddress> servers;

    servers.push_back(GridClientSocketAddress("127.0.0.1", 11212));

    clientConfig.servers(servers);

    GridClientProtocolConfiguration protoCfg;

    protoCfg.protocol(TCP);

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

class PortablePerson : public GridPortable {
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

    void writePortable(GridPortableWriter &writer) const {
        writer.writeString("name", name);
		writer.writeInt32("age", id);
	}

    void readPortable(GridPortableReader &reader) {
		name = reader.readString("name");
        id = reader.readInt32("id");
	}

    bool operator==(const GridPortable& portable) const {
        const PortablePerson* other = static_cast<const PortablePerson*>(&portable);

        return id == other->id;
    }

    int hashCode() const {
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
        string name = reader.readString("name");

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

BOOST_AUTO_TEST_CASE(testClient) {
    GridClientConfiguration cfg = clientConfig();

	TGridClientPtr client = GridClientFactory::start(cfg);	

    TGridClientDataPtr data = client->data("partitioned");
}

/*
BOOST_AUTO_TEST_CASE(testPortableCache) {
    GridClientConfiguration cfg = clientConfig();

	TGridClientPtr client = GridClientFactory::start(cfg);	

    TGridClientDataPtr data = client->data("partitioned");

    PortablePerson person1(10, "person1");

    data->put(10, &person1);

    PortablePerson* getPerson1 = data->get(10).getPortable<PortablePerson>();

    cout << "Person1: " << getPerson1->getId();

    PersonSerializer serializer;

    Person person2(20, "person2");

    data->put(20, &GridExternalPortable<Person>(&person2, serializer));

    GridExternalPortable<Person>* getPerson2 = data->get(10).getPortable<GridExternalPortable<Person>>();

    cout << "Person2: " << (*getPerson2)->getId();
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
*/

BOOST_AUTO_TEST_SUITE_END()
