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

#include <boost/shared_ptr.hpp>
#include <boost/test/unit_test.hpp>

#include <gridgain/gridgain.hpp>

#include "gridgain/gridclientvariant.hpp"

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

BOOST_AUTO_TEST_SUITE(GridClientPortable)

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

class TestPortable : public GridPortable {
public:    
	int32_t typeId() const {
		return 7;
	}

    ~TestPortable() {
        cout << "removed\n";
    }

    void writePortable(GridPortableWriter &writer) const {
		writer.writeInt("id", 10);

        string str("abc");

        writer.writeString("name", str);
	}

    void readPortable(GridPortableReader &reader) {
		reader.readInt("id");

        string str = reader.readString("name");
	}

    bool operator==(const GridPortable& other) const {
        return true;
    }
};

class Deserializer {public: virtual void* newInstance() = 0; };

unordered_map<int32_t, Deserializer*> m;

#define REGISTER_TYPE(TYPE_ID, TYPE) class fun_##TYPE : public Deserializer { public: fun_##TYPE() {m[TYPE_ID] = this;} void* newInstance() { return new TYPE;}; }; fun_##TYPE var_##TYPE;

REGISTER_TYPE(1, TestPortable);

void test2(GridClientVariant var) {
    cout << "test2\n";
}

void test() {
    GridClientVariant key1(new TestPortable());

    test2(key1);
}

BOOST_AUTO_TEST_CASE(testPortableMarshalling) {
	/*
    GridPortableMarshaller marsh;

    Deserializer* d = m[1];

    GridPortable* p = (GridPortable*)d->newInstance();
    
    cout << p;
    */
    
    /*
    unordered_map<TestPortable, TestPortable> m;

    TestPortable t1;
    TestPortable t2;

    m[t1] = t2;
    */
    
    /*
    unordered_map<string, string> m;

    string t1("a");
    string t2("b");

    m[t1] = t2;

    */

    /*
	TestPortable test;

    vector<int8_t> bytes = marsh.marshal(test);

	cout << "done " << bytes.size() << "\n";

	for (int i = 0; i < bytes.size(); i++)
		cout << ((int)bytes[i]) << " ";

	cout << "\n";

    GridClientConfiguration cfg = clientConfig();

	TGridClientPtr client = GridClientFactory::start(cfg);	

    TGridClientDataPtr data = client->data("partitioned");

    data->put(1, 1);
    */

    /*
	GridClientConfiguration cfg = clientConfig();

	TGridClientPtr client = GridClientFactory::start(cfg);	

    TGridClientDataPtr data = client->data("partitioned");

	GridClientVariant key1(new TestPortable());
	GridClientVariant val1(new TestPortable());

	data->put(key1, val1);

    TestPortable* p = (TestPortable*)key1.getPortable();

    data->put(p, p);

    string s = data->get("a").getString();

    TGridClientVariantMap pmap;

    pmap[key1] = val1;
    */

    /*
    TestPortable* p = new TestPortable();

    delete p;

    cout << "test\n";

    test();

    cout << "test end \n";
    */
    
	GridClientVariant key1(new TestPortable());
	GridClientVariant val1(new TestPortable());

    GridClientVariant key2(new TestPortable());
	GridClientVariant val2(new TestPortable());

    TGridClientVariantMap pmap;

    pmap[key1] = val1;
    pmap[key1] = val2;
    
}

BOOST_AUTO_TEST_SUITE_END()
