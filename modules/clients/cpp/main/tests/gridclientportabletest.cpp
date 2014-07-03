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

std::set<GridClientCacheFlag> keepPortable() {
    std::set<GridClientCacheFlag> flags;

    flags.insert(GridClientCacheFlag::KEEP_PORTABLE);

    return flags;
}

class GridClientTestPortable : public GridPortable {
public:
    GridClientTestPortable() : portable1(0), portable2(0), portableRaw1(0), portableRaw2(0), date(0), dateRaw(0) {
    }

    GridClientTestPortable(int32_t val, bool createNested) : b(val), s(val), i(val), l(val), date(val),
        dateRaw(val + 1) {
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
        writer.writeByteArray("_bArr", bArr.data(), bArr.size());
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
        writer.writeVariantArray("_objArr", objArr.begin(), objArr.end());
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
        raw.writeVariantCollection(colRaw);
        raw.writeVariantMap(mapRaw);

        if (portableRaw1)
            raw.writeVariant(portableRaw1);
        else
            raw.writeVariant(GridClientVariant());

        if (portableRaw2)
            raw.writeVariant(portableRaw2);
        else
            raw.writeVariant(GridClientVariant());
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
        reader.readByteArray("_bArr", bArr);
        reader.readInt16Array("_sArr", sArr);
        reader.readInt32Array("_iArr", iArr);
        reader.readInt64Array("_lArr", lArr);
        reader.readFloatArray("_fArr", fArr);
        reader.readDoubleArray("_dArr", dArr);
        reader.readCharArray("_cArr", cArr);
        reader.readBoolArray("_boolArr", boolArr);
        reader.readStringArray("_strArr", strArr);
        reader.readUuidArray("_uuidArr", uuidArr);
        reader.readDateArray("_dateArr", dateArr);
        reader.readVariantArray("_objArr", objArr);
        reader.readVariantCollection("_col", col);
        reader.readVariantMap("_map", map);

        GridClientVariant var = reader.readVariant("_portable1");
        portable1 = var.deserializePortable<GridClientTestPortable>();

        var = reader.readVariant("_portable2");
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
        raw.readByteArray(bArrRaw);
        raw.readInt16Array(sArrRaw);
        raw.readInt32Array(iArrRaw);
        raw.readInt64Array(lArrRaw);
        raw.readFloatArray(fArrRaw);
        raw.readDoubleArray(dArrRaw);
        raw.readCharArray(cArrRaw);
        raw.readBoolArray(boolArrRaw);
        raw.readStringArray(strArrRaw);
        raw.readUuidArray(uuidArrRaw);
        raw.readDateArray(dateArrRaw);
        raw.readVariantArray(objArrRaw);
        raw.readVariantCollection(colRaw);
        raw.readVariantMap(mapRaw);

        var = raw.readVariant();
        portableRaw1 = var.deserializePortable<GridClientTestPortable>();

        var = raw.readVariant();
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

REGISTER_TYPE(GridClientTestPortable);

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

REGISTER_TYPE(TestPortableKey);

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

REGISTER_TYPE(TestPortableValue);

boost::mutex testCheckMux;

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

    TGridClientDataPtr data = client->data(CACHE_NAME)->flagsOn(keepPortable());

    GridClientVariant portable = data->get(1);

    std::unique_ptr<GridClientTestPortable> ptr = portable.deserializePortableUnique<GridClientTestPortable>();

    checkGridClientTestPortable(100, 2, ptr.get(), true);
}

BOOST_FIXTURE_TEST_CASE(testPutGetPortable, GridClientFactoryFixture1<clientConfig>) {
    TGridClientDataPtr data = client->data(CACHE_NAME)->flagsOn(keepPortable());

    multithreaded([&] {
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

            if (i % 100 == 0)
                cout << "Run iteration " << i << "\n";
        }
    });
}

BOOST_FIXTURE_TEST_CASE(testPutAllGetAllPortable, GridClientFactoryFixture1<clientConfig>) {
    TGridClientDataPtr data = client->data(CACHE_NAME)->flagsOn(keepPortable());

    multithreaded([&] {
        TestPortableKey invalidKey(-1);

        for (int i = 0; i < 100; i++) {
            vector<TestPortableKey> keys;

            vector<TestPortableValue> vals;

            for (int j = i; j < i + 100; j++) {
                keys.push_back(TestPortableKey(j));

                vals.push_back(TestPortableValue(j, "string"));
            }

            TGridClientVariantMap map;

            for (int j = 0; j < 100; j++)
                map[GridClientVariant(&keys[j])] = GridClientVariant(&vals[j]);

            data->putAll(map);

            TGridClientVariantSet keyVars;

            for (int j = 0; j < 100; j++)
                keyVars.push_back(GridClientVariant(&keys[j]));

            keyVars.push_back(GridClientVariant(&invalidKey));

            TGridClientVariantMap getMap = data->getAll(keyVars);

            if (getMap.size() != 100)
                BOOST_FAIL("Unexpected result size.");
            else {
                TGridClientVariantMap map;

                vector<std::shared_ptr<TestPortableKey>> keyPtrs;

                for (auto iter = getMap.begin(); iter != getMap.end(); ++iter) {
                    std::shared_ptr<TestPortableKey> keyPtr(iter->first.deserializePortable<TestPortableKey>());

                    GridClientVariant key = GridClientVariant(keyPtr.get());

                    map.emplace(std::make_pair(std::move(key), iter->second));

                    keyPtrs.push_back(keyPtr);
                }

                for (int j = 0; j < 100; j++) {
                    GridClientVariant getVal = map[keyVars[j]];

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

                if (i % 5 == 0)
                    cout << "Run iteration " << i << "\n";
            }
        }
    });
}

BOOST_FIXTURE_TEST_CASE(testPortableTaskArg, GridClientFactoryFixture1<clientConfig>) {
    TGridClientComputePtr compute = client->compute();

    multithreaded([&] {
        for (int i = 0; i < 500; i++) {
            int testVal = i % 100;

            GridClientTestPortable arg(testVal, true);

            GridClientVariant taskArg(&arg);

            GridClientVariant res = compute->execute("org.gridgain.client.GridClientPortableArgumentTask", taskArg);

            if (!res.hasAnyValue())
                BOOST_FAIL("Failed to get value.");
            else {
                std::unique_ptr<GridClientTestPortable> val = res.deserializePortableUnique<GridClientTestPortable>();

                boost::lock_guard<boost::mutex> g(testCheckMux);

                checkGridClientTestPortable(testVal + 1, 2, val.get(), true);
            }

            if (i % 100 == 0)
                cout << "Run iteration " << i << "\n";
        }
    });
}

BOOST_FIXTURE_TEST_CASE(testPutGetDetached, GridClientFactoryFixture1<clientConfig>) {
    GridClientTestPortable obj1(1, false);
    GridClientTestPortable obj2(2, false);

    GridClientTestPortable obj3(3, false);

    obj2.portable1 = &obj1;
    obj3.portable1 = &obj1;

    TGridClientVariantMap map;

    map[0] = GridClientVariant(&obj1);
    map[1] = GridClientVariant(&obj2);
    map[2] = GridClientVariant(&obj3);

    TGridClientDataPtr data = client->data(CACHE_NAME)->flagsOn(keepPortable());

    data->putAll(map); // During put obj2 and obj3 reference the same obj1.

    obj2.portable1 = 0;
    obj3.portable1 = 0;

    GridPortableObject p1 = data->get(0).getPortableObject();
    GridPortableObject p2 = data->get(1).getPortableObject();
    GridPortableObject p3 = data->get(2).getPortableObject();

    BOOST_REQUIRE_EQUAL(1, p1.field("_i").getInt());
    BOOST_REQUIRE(!p1.field("_portable1").hasAnyValue());

    GridPortableObject p21 = p2.field("_portable1").getPortableObject();
    GridPortableObject p31 = p3.field("_portable1").getPortableObject();

    BOOST_REQUIRE_EQUAL(1, p21.field("_i").getInt());
    BOOST_REQUIRE_EQUAL(1, p31.field("_i").getInt());

    BOOST_REQUIRE(p21 == p31);

    std::unique_ptr<GridClientTestPortable> get1 = p1.deserializeUnique<GridClientTestPortable>();
    BOOST_REQUIRE_EQUAL(1, get1->i);

    std::unique_ptr<GridClientTestPortable> get2 = p2.deserializeUnique<GridClientTestPortable>();
    BOOST_REQUIRE_EQUAL(2, get2->i);

    std::unique_ptr<GridClientTestPortable> get3 = p3.deserializeUnique<GridClientTestPortable>();
    BOOST_REQUIRE_EQUAL(3, get3->i);

    BOOST_REQUIRE(get2->portable1);
    BOOST_REQUIRE_EQUAL(1, get2->portable1->i);

    BOOST_REQUIRE(get3->portable1);
    BOOST_REQUIRE_EQUAL(1, get3->portable1->i);
}

BOOST_FIXTURE_TEST_CASE(testPutPortable, GridClientFactoryFixture1<clientConfig>) {
    GridClientTestPortable obj1(1, false);
    GridClientTestPortable obj2(2, true);

    TGridClientDataPtr data = client->data(CACHE_NAME)->flagsOn(keepPortable());

    data->put(1, &obj1);
    data->put(2, &obj2);

    GridPortableObject p1 = data->get(1).getPortableObject();

    data->put(3, p1);

    GridPortableObject p11 = data->get(3).getPortableObject();

    BOOST_REQUIRE_EQUAL(1, p11.field("_i").getInt());
    BOOST_REQUIRE(p1 == p11);

    GridPortableObject p2 = data->get(2).getPortableObject();

    BOOST_REQUIRE(p2.field("_portable1").hasPortableObject());
    BOOST_REQUIRE_EQUAL(3, p2.field("_portable1").getPortableObject().field("_i").getInt());

    data->put(4, p2);

    GridPortableObject p21 = data->get(4).getPortableObject();

    BOOST_REQUIRE(p2 == p21);
    BOOST_REQUIRE_EQUAL(2, p21.field("_i").getInt());
    BOOST_REQUIRE_EQUAL(3, p21.field("_portable1").getPortableObject().field("_i").getInt());
}

BOOST_FIXTURE_TEST_CASE(testKeepPortable, GridClientFactoryFixture1<clientConfig>) {
    GridClientTestPortable obj1(1, false);
    GridClientTestPortable obj2(2, false);

    TGridClientDataPtr data = client->data(CACHE_NAME);

    data->put(1, &obj1);
    data->put(2, &obj2);
    data->put(3, 1);

    GridClientVariant res = data->get(1);

    BOOST_REQUIRE(res.hasPortable());
    BOOST_REQUIRE(!res.hasPortableObject());

    BOOST_REQUIRE_EQUAL(1, (res.getPortable<GridClientTestPortable>())->i);

    delete res.getPortable();

    res = data->get(3);

    BOOST_REQUIRE(res.hasInt());

    vector<GridClientVariant> keys;

    keys.push_back(1);
    keys.push_back(2);
    keys.push_back(3);

    TGridClientVariantMap map = data->getAll(keys);

    BOOST_REQUIRE_EQUAL(3, map.size());

    res = map[1];

    BOOST_REQUIRE(res.hasPortable());
    delete res.getPortable();

    res = map[3];

    BOOST_REQUIRE(res.hasInt());

    data = data->flagsOn(keepPortable());

    res = data->get(1);

    BOOST_REQUIRE(!res.hasPortable());
    BOOST_REQUIRE(res.hasPortableObject());

    BOOST_REQUIRE_EQUAL(1, res.getPortableObject().field("_i").getInt());

    map = data->getAll(keys);

    BOOST_REQUIRE_EQUAL(3, map.size());

    res = map[1];

    BOOST_REQUIRE(res.hasPortableObject());
    BOOST_REQUIRE_EQUAL(1, res.getPortableObject().field("_i").getInt());

    res = map[3];

    BOOST_REQUIRE(res.hasInt());

    data = data->flagsOff(keepPortable());
    
    res = data->get(1);

    BOOST_REQUIRE(res.hasPortable());
    BOOST_REQUIRE(!res.hasPortableObject());

    delete res.getPortable();
}

BOOST_AUTO_TEST_SUITE_END()
