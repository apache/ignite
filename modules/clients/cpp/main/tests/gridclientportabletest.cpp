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
#include <gridgain/gridclienthash.hpp>

#include "gridclientfactoryfixture.hpp"
#include "gridtestcommon.hpp"

using namespace std;

BOOST_AUTO_TEST_SUITE(GridClientPortableIntegrationSuite)

static string SERVER_ADDRESS = "127.0.0.1";
static string CACHE_NAME = "partitioned";
static int KEYS_CNT = 10;
static int TEST_TCP_PORT = 10080;
static string CREDS = "s3cret";

GridClientConfiguration clientConfig() {
    GridClientConfiguration clientConfig;

    vector<GridClientSocketAddress> servers;

    servers.push_back(GridClientSocketAddress(SERVER_ADDRESS, TEST_TCP_PORT));

    clientConfig.servers(servers);

    GridClientProtocolConfiguration protoCfg;

    protoCfg.credentials(CREDS);

    clientConfig.protocolConfiguration(protoCfg);

    return clientConfig;
}

class GridClientTestPortable : public GridPortable {
public:
	GridClientTestPortable() : portable1(0), portable2(0), portableRaw1(0), portableRaw2(0), date(0), dateRaw(0) {
    }

    GridClientTestPortable(int32_t val, bool createNested) : date(val), dateRaw(val + 1) {
        if (createNested) {
            portable1 = new GridClientTestPortable(val + 1, false);
            portable2 = portable1;

            portableRaw1 = new GridClientTestPortable(val + 2, false);
            portableRaw2 = portableRaw1;
        }
        else {
            portable1 = 0;
            portable2 = 0;
            portableRaw1 = 0;
            portableRaw2 = 0;
        }
    }

    ~GridClientTestPortable() {
        if (portable1) {
            if (portable1 == portable2)
                portable2 = 0;

            delete portable1;
        }

        if (portable2)
            delete portable2;

        if (portableRaw1) {
            if (portableRaw1 == portableRaw2)
                portableRaw2 = 0;

            delete portableRaw1;
        }

        if (portableRaw2)
            delete portableRaw2;
    }

    int32_t typeId() const {
		return 10000;
	}

    void writePortable(GridPortableWriter &writer) const {
        writer.writeByte("_b", b);
        writer.writeInt16("_s", s);
        writer.writeInt32("_i", i);
        writer.writeInt64("_l", l);
        writer.writeFloat("_f", f);
        writer.writeDouble("_d", d);
        writer.writeChar("_c", c);
        writer.writeBool("_bool", boolVal);
        writer.writeString("_str", str);
        writer.writeUuid("_uuid", uuid);
        writer.writeDate("_date", date);
        writer.writeByteCollection("_bArr", bArr);
        writer.writeInt16Collection("_sArr", sArr);
        writer.writeInt32Collection("_iArr", iArr);
        writer.writeInt64Collection("_lArr", lArr);
        writer.writeFloatCollection("_fArr", fArr);
        writer.writeDoubleCollection("_dArr", dArr);
        writer.writeCharCollection("_cArr", cArr);
        writer.writeBoolCollection("_boolArr", boolArr);
        writer.writeStringCollection("_strArr", strArr);
        writer.writeUuidCollection("_uuidArr", uuidArr);
        writer.writeDateCollection("_dateArr", dateArr);
        writer.writeVariantCollection("_objArr", objArr);
        writer.writeVariantCollection("_col", col);
        writer.writeVariantMap("_map", map);
        
        if (portable1)
            writer.writeVariant("_portable1", portable1);
        if (portable2)
            writer.writeVariant("_portable2", portable2);

        GridPortableRawWriter& raw = writer.rawWriter();

        raw.writeByte(bRaw);
        raw.writeInt16(sRaw);
        raw.writeInt32(iRaw);
        raw.writeInt64(lRaw);
        raw.writeFloat(fRaw);
        raw.writeDouble(dRaw);
        raw.writeChar(cRaw);
        raw.writeBool(boolValRaw);
        raw.writeString(strRaw);
        raw.writeUuid(uuidRaw);
        raw.writeDate(dateRaw);
        raw.writeByteCollection(bArrRaw);
        raw.writeInt16Collection(sArrRaw);
        raw.writeInt32Collection(iArrRaw);
        raw.writeInt64Collection(lArrRaw);
        raw.writeFloatCollection(fArrRaw);
        raw.writeDoubleCollection(dArrRaw);
        raw.writeCharCollection(cArrRaw);
        raw.writeBoolCollection(boolArrRaw);
        raw.writeStringCollection(strArrRaw);
        raw.writeUuidCollection(uuidArrRaw);
        raw.writeDateCollection(dateArrRaw);
        raw.writeVariantCollection(objArrRaw);
        raw.writeVariantCollection(colRaw);
        raw.writeVariantMap(mapRaw);

        if (portableRaw1)
            raw.writeVariant(portableRaw1);
        if (portableRaw2)
            raw.writeVariant(portableRaw2);
	}

    void readPortable(GridPortableReader &reader) {
        b = reader.readByte("_b");
        s = reader.readInt16("_s");
        i = reader.readInt32("_i");
        l = reader.readInt64("_l");
        f = reader.readFloat("_f");
        d = reader.readDouble("_d");
        c = reader.readChar("_c");
        boolVal = reader.readBool("_bool");
        str = reader.readString("_str").get();
        uuid = reader.readUuid("_uuid").get();
        date = reader.readDate("_date").get();
        bArr = reader.readByteCollection("_bArr").get();
        sArr = reader.readInt16Collection("_sArr").get();
        iArr = reader.readInt32Collection("_iArr").get();
        lArr = reader.readInt64Collection("_lArr").get();
        fArr = reader.readFloatCollection("_fArr").get();
        dArr = reader.readDoubleCollection("_dArr").get();
        cArr = reader.readCharCollection("_cArr").get();
        boolArr = reader.readBoolCollection("_boolArr").get();
        strArr = reader.readStringCollection("_strArr").get();
        uuidArr = reader.readUuidCollection("_uuidArr").get();
        dateArr = reader.readDateCollection("_dateArr").get();
        objArr = reader.readVariantCollection("_objArr").get();
        col = reader.readVariantCollection("_col").get();
        map = reader.readVariantMap("_map").get();
        
        GridClientVariant var = reader.readVariant("_portable1");
        if (var.hasPortableObject())
            portable1 = var.deserializePortable<GridClientTestPortable>();

        var = reader.readVariant("_portable2");
        if (var.hasPortableObject())
            portable2 = var.deserializePortable<GridClientTestPortable>();

        GridPortableRawReader& raw = reader.rawReader();

        bRaw = raw.readByte();
        sRaw = raw.readInt16();
        iRaw = raw.readInt32();
        lRaw = raw.readInt64();
        fRaw = raw.readFloat();
        dRaw = raw.readDouble();
        cRaw = raw.readChar();
        boolValRaw = raw.readBool();
        strRaw = raw.readString().get();
        uuidRaw = raw.readUuid().get();
        dateRaw = raw.readDate().get();
        bArrRaw = raw.readByteCollection().get();
        sArrRaw = raw.readInt16Collection().get();
        iArrRaw = raw.readInt32Collection().get();
        lArrRaw = raw.readInt64Collection().get();
        fArrRaw = raw.readFloatCollection().get();
        dArrRaw = raw.readDoubleCollection().get();
        cArrRaw = raw.readCharCollection().get();
        boolArrRaw = raw.readBoolCollection().get();
        strArrRaw = raw.readStringCollection().get();
        uuidArrRaw = raw.readUuidCollection().get();
        dateArrRaw = raw.readDateCollection().get();
        objArrRaw = raw.readVariantCollection().get();
        colRaw = raw.readCollection().get();
        mapRaw = raw.readVariantMap().get();

        var = raw.readVariant();
        if (var.hasPortableObject())
            portableRaw1 = var.deserializePortable<GridClientTestPortable>();
    
        var = raw.readVariant();
        if (var.hasPortableObject())
            portableRaw2 = var.deserializePortable<GridClientTestPortable>();
	}

    /** */
    int8_t b;

    /** */
    int8_t bRaw;

    /** */
    int16_t s;

    /** */
    int16_t sRaw;

    /** */
    int32_t i;

    /** */
    int32_t iRaw;

    /** */
    int64_t l;

    /** */
    int64_t lRaw;

    /** */
    float f;

    /** */
    float fRaw;

    /** */
    double d;

    /** */
    double dRaw;

    /** */
    uint16_t c;

    /** */
    uint16_t cRaw;

    /** */
    bool boolVal;

    /** */
    bool boolValRaw;

    /** */
    string str;

    /** */
    string strRaw;

    /** */
    GridClientUuid uuid;

    /** */
    GridClientUuid uuidRaw;

    /** */
    GridClientDate date;

    /** */
    GridClientDate dateRaw;

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
    vector<string> strArrRaw;

    /** */
    vector<GridClientUuid> uuidArr;

    /** */
    vector<GridClientUuid> uuidArrRaw;

    /** */
    vector<boost::optional<GridClientDate>> dateArr;

    /** */
    vector<boost::optional<GridClientDate>> dateArrRaw;

    /** */
    TGridClientVariantSet objArr;

    /** */
    TGridClientVariantSet objArrRaw;

    /** */
    TGridClientVariantSet col;

    /** */
    TGridClientVariantSet colRaw;

    /** */
    TGridClientVariantMap map;

    /** */
    TGridClientVariantMap mapRaw;

    /** */
    GridClientTestPortable* portable1;

    /** */
    GridClientTestPortable* portable2;

    /** */
    GridClientTestPortable* portableRaw1;

    /** */
    GridClientTestPortable* portableRaw2;
};

REGISTER_TYPE(10000, GridClientTestPortable);

class TestPortableKey : public GridHashablePortable {
public:
    TestPortableKey() {
    }

    TestPortableKey(int64_t id) : id(id) {
    }

    int32_t typeId() const {
        return 10001;
    }

    void writePortable(GridPortableWriter& writer) const {
        writer.writeInt64("id", id);
    }

    void readPortable(GridPortableReader& reader) {
        id = reader.readInt64("id");
    }

    int32_t hashCode() const {
        return gridInt16Hash(id);
    }

    bool operator==(const GridHashablePortable& other) const {
        return id == static_cast<const TestPortableKey*>(&other)->id;
    }

    int64_t id;
};

REGISTER_TYPE(10001, TestPortableKey);

class TestPortableValue : public GridPortable {
public:    
    TestPortableValue() {
    }

    TestPortableValue(int32_t i, string s) : i(i), s(s) {
    }

    int32_t typeId() const {
        return 10002;
    }

    void writePortable(GridPortableWriter& writer) const {
        writer.writeInt32("i", i);
        writer.writeString("s", s);
    }

    void readPortable(GridPortableReader& reader) {
        i = reader.readInt32("i");
        
        boost::optional<string> sOpt = reader.readString("s");

        if (sOpt.is_initialized())
            s = sOpt.get();
    }

    int32_t i;
    
    string s;
};

REGISTER_TYPE(10002, TestPortableValue);

void checkGridClientTestPortable(int32_t val, int32_t arrSize, GridClientTestPortable* ptr, bool nested) {
    BOOST_REQUIRE_EQUAL(val, ptr->b);
    BOOST_REQUIRE_EQUAL(val, ptr->s);
    BOOST_REQUIRE_EQUAL(val, ptr->i);
    BOOST_REQUIRE_EQUAL(val, ptr->l);
    BOOST_REQUIRE_EQUAL(val + 0.5f, ptr->f);
    BOOST_REQUIRE_EQUAL(val + 0.5, ptr->d);
    BOOST_REQUIRE_EQUAL(val, ptr->c);
    BOOST_REQUIRE_EQUAL(true, ptr->boolVal);
    BOOST_REQUIRE(boost::lexical_cast<std::string>(val) == ptr->str);
    BOOST_REQUIRE(GridClientUuid(val, val) == ptr->uuid);
    BOOST_REQUIRE(GridClientDate(val) == ptr->date);

    BOOST_REQUIRE_EQUAL(val + 1, ptr->bRaw);
    BOOST_REQUIRE_EQUAL(val + 1, ptr->sRaw);
    BOOST_REQUIRE_EQUAL(val + 1, ptr->iRaw);
    BOOST_REQUIRE_EQUAL(val + 1, ptr->lRaw);
    BOOST_REQUIRE_EQUAL(val + 1.5f, ptr->fRaw);
    BOOST_REQUIRE_EQUAL(val + 1.5, ptr->dRaw);
    BOOST_REQUIRE_EQUAL(val + 1, ptr->cRaw);
    BOOST_REQUIRE_EQUAL(false, ptr->boolValRaw);
    BOOST_REQUIRE(boost::lexical_cast<std::string>(val + 1) == ptr->strRaw);
    BOOST_REQUIRE(GridClientUuid(val + 1, val + 1) == ptr->uuidRaw);
    BOOST_REQUIRE(GridClientDate(val + 1) == ptr->dateRaw);

    BOOST_REQUIRE_EQUAL(2, ptr->bArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->sArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->iArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->lArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->fArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->dArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->cArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->boolArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->strArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->uuidArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->dateArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->objArr.size());
    BOOST_REQUIRE_EQUAL(2, ptr->col.size());
    BOOST_REQUIRE_EQUAL(2, ptr->map.size());

    BOOST_REQUIRE_EQUAL(2, ptr->bArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->sArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->iArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->lArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->fArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->dArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->cArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->boolArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->strArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->uuidArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->dateArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->objArrRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->colRaw.size());
    BOOST_REQUIRE_EQUAL(2, ptr->mapRaw.size());

    if (nested) {
        BOOST_REQUIRE(ptr->portable1);
        BOOST_REQUIRE(ptr->portable2);
        BOOST_REQUIRE(ptr->portableRaw1);
        BOOST_REQUIRE(ptr->portableRaw2);
    
        BOOST_REQUIRE_EQUAL(ptr->portable1, ptr->portable2);
        BOOST_REQUIRE_EQUAL(ptr->portableRaw1, ptr->portableRaw2);

        checkGridClientTestPortable(val + 1, arrSize, ptr->portable1, false);
        checkGridClientTestPortable(val + 2, arrSize, ptr->portableRaw1, false);
    }
    else {
        BOOST_REQUIRE(ptr->portable1 == 0);
        BOOST_REQUIRE(ptr->portable2 == 0);
        BOOST_REQUIRE(ptr->portableRaw1 == 0);
        BOOST_REQUIRE(ptr->portableRaw2 == 0);
    }
}

BOOST_FIXTURE_TEST_CASE(testCreateOnJava, GridClientFactoryFixture1<clientConfig>) {
    TGridClientComputePtr compute = client->compute();

    GridClientVariant res = compute->execute("org.gridgain.client.GridClientPutPortableTask", CACHE_NAME);

    TGridClientDataPtr data = client->data(CACHE_NAME);

    GridClientVariant portable = data->get(1);

    std::unique_ptr<GridClientTestPortable> ptr = portable.deserializePortableUnique<GridClientTestPortable>();

    checkGridClientTestPortable(100, 2, ptr.get(), true);
}

BOOST_FIXTURE_TEST_CASE(testPutGetPortable, GridClientFactoryFixture1<clientConfig>) {
    TGridClientDataPtr data = client->data(CACHE_NAME);

    multithreaded([&] {
        // TODO 
        for (int i = 0; i < 1000; i++) {
            TestPortableKey key(i);
            TestPortableValue val(i, "string");
        
            GridClientVariant varKey(&key);
            GridClientVariant varValue(&val);

            data->put(varKey, varValue);
            
            GridClientVariant getVal = data->get(varKey);

            if (!getVal.hasAnyValue())
                BOOST_FAIL("Failed to get value.");
            else {
                std::unique_ptr<TestPortableValue> val = getVal.deserializePortableUnique<TestPortableValue>();

                if (val->i != i)
                    BOOST_FAIL("Read invalid i.");

                if (val->s != "string")
                    BOOST_FAIL("Read invalid s.");
            }
        }
    }, 1);
}

BOOST_FIXTURE_TEST_CASE(testPutAllGetAllPortable, GridClientFactoryFixture1<clientConfig>) {
    TGridClientDataPtr data = client->data(CACHE_NAME);

    multithreaded([&] {
        // TODO 
        TestPortableKey invalidKey(-1);

        for (int i = 0; i < 1; i++) {
            vector<TestPortableKey> keys(100);

            vector<TestPortableValue> vals(100);
            
            for (int j = i; j < i + 100; j++) {
                keys.push_back(TestPortableKey(j));

                vals.push_back(TestPortableValue(j, "string"));
            }

            TGridClientVariantMap map;
            
            for (int j = 0; j < 100; j++)
                map[GridClientVariant(&keys[j])] = GridClientVariant(&vals[j]);
        
            data->putAll(map);
            
            TGridClientVariantSet keyVars(101);

            for (int j = 0; j < 100; j++)
                keyVars.push_back(GridClientVariant(&keys[j]));

            keyVars.push_back(GridClientVariant(&invalidKey));

            TGridClientVariantMap getMap = data->getAll(keyVars);

            if (getMap.size() != 100)
                BOOST_FAIL("Unexpected result size.");
            else {
                for (int j = 0; j < 100; j++) {
                    GridClientVariant getVal = getMap[keyVars[j]];

                    if (!getVal.hasAnyValue())
                        BOOST_FAIL("Failed to get value.");
                    else {
                        std::unique_ptr<TestPortableValue> val = getVal.deserializePortableUnique<TestPortableValue>();

                        if (val->i != (i + j))
                            BOOST_FAIL("Read invalid i.");

                        if (val->s != "string")
                            BOOST_FAIL("Read invalid s.");
                    }
                }
            }
        }
    }, 1);
}

BOOST_FIXTURE_TEST_CASE(testPortableTaskArg, GridClientFactoryFixture1<clientConfig>) {
    TGridClientComputePtr compute = client->compute();

    multithreaded([&] {
        // TODO 
        for (int i = 0; i < 1; i++) {
            GridClientTestPortable arg(i, true);
        
            GridClientVariant taskArg(&arg);

            GridClientVariant res = compute->execute("org.gridgain.client.GridClientPortableArgumentTask", taskArg);
            
            if (!res.hasAnyValue())
                BOOST_FAIL("Failed to get value.");
            else {
                std::unique_ptr<GridClientTestPortable> val = res.deserializePortableUnique<GridClientTestPortable>();

                checkGridClientTestPortable(i + 1, 2, val.get(), true);
            }
        }
    }, 1);
}

BOOST_AUTO_TEST_SUITE_END()