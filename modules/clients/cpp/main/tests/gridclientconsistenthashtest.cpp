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

#include <boost/test/unit_test.hpp>
#include <boost/array.hpp>
#include <string>
#include <sstream>

#include "gridgain/impl/gridclientpartitionedaffinity.hpp"
#include "gridgain/impl/hash/gridclientsimpletypehasheableobject.hpp"
#include "gridgain/impl/hash/gridclientstringhasheableobject.hpp"
#include "gridgain/impl/hash/gridclientboolhasheableobject.hpp"
#include "gridgain/impl/hash/gridclientdoublehasheableobject.hpp"
#include "gridgain/impl/hash/gridclientvarianthasheableobject.hpp"
#include "gridgain/impl/hash/gridclientfloathasheableobject.hpp"
#include "gridgain/gridclientnode.hpp"
#include "gridgain/impl/utils/gridclientlog.hpp"

using namespace std;

static void checkHash(const char* testVal, const GridClientHasheableObject& o, const int8_t* hash, size_t nBytes, int code);
static void checkUUID(const string& uuid, int code);

#ifdef _MSC_VER

#include <windows.h>

string toUtf8String(const wstring& s) {
    CHAR buf[512];

    WideCharToMultiByte(
        CP_UTF8, //UTF-8 encoding
        0,
        s.c_str(), // the string you have
        -1, // length of the string - set -1 to indicate it is null terminated
        buf, // output
        sizeof(buf), // size of the buffer in bytes - if you leave it zero the return value is the length required for the output buffer
        NULL,
        NULL
    );

    return string(buf);
}

wstring getStringFromResource(UINT id) {
    const int BUF_SIZE = 512;
    TCHAR buf[BUF_SIZE];

    int rc = LoadString(GetModuleHandle(NULL), id, buf, BUF_SIZE);

    if (rc > 0) {
        return wstring(buf);
    }
    else {
        throw exception("Failed to load string.");
    }
}

#endif

BOOST_AUTO_TEST_SUITE(GridClientConsistenceHashSelftTest)

BOOST_AUTO_TEST_CASE(testHashGeneraton) {
#ifndef _MSC_VER
    static const int8_t EMPTY[] = { };

    ::setlocale(LC_ALL, ""); // Set locale for proper Unicode strings conversion.

    // Validate strings.
    checkHash("EMPTY", GridStringHasheableObject(""), EMPTY, sizeof(EMPTY) / sizeof(*EMPTY), -1484017934);
    checkHash("EMPTY-VAR", GridClientVariantHasheableObject(GridClientVariant("")), EMPTY,
            sizeof(EMPTY) / sizeof(*EMPTY), -1484017934);

    {
        int8_t bytes[] = { 72, 97, 100, 111, 111, 112, -29, -126, -110, -26, -82, -70, -29, -127, -103 };

        checkHash(
            "Hadoop",
            GridWideStringHasheableObject(L"Hadoop\u3092\u6bba\u3059"), // Hadoopを殺す
            bytes,
            sizeof(bytes) / sizeof(*bytes),
            -695300527);
        checkHash(
            "Hadoop-VAR", GridClientVariantHasheableObject(
            GridClientVariant(wstring(L"Hadoop\u3092\u6bba\u3059"))),
            bytes,
            sizeof(bytes) / sizeof(*bytes),
            -695300527);
    }
#endif

    {
        int8_t bytes[] = { 49 };

        checkHash("1", GridStringHasheableObject("1"), bytes, sizeof(bytes) / sizeof(*bytes), -80388575);
        checkHash("1-VAR", GridClientVariantHasheableObject(GridClientVariant("1")), bytes,
                sizeof(bytes) / sizeof(*bytes), -80388575);
    }

    {
        int8_t bytes[] = { 97 };

        checkHash("a", GridStringHasheableObject("a"), bytes, sizeof(bytes) / sizeof(*bytes), -873690096);
        checkHash("a-VAR", GridClientVariantHasheableObject(GridClientVariant("a")), bytes,
                sizeof(bytes) / sizeof(*bytes), -873690096);
    }

    {
        int8_t bytes[] = { 107, 101, 121, 49};

        checkHash("key1", GridStringHasheableObject("key1"), bytes, sizeof(bytes) / sizeof(*bytes), -2067461682);
        checkHash("key1-VAR", GridClientVariantHasheableObject(GridClientVariant("key1")), bytes,
                sizeof(bytes) / sizeof(*bytes), -2067461682);
    }

    // Validate primitives
    {
        int8_t bytes[] = { -49, 4, 0, 0 };

        checkHash("bool - true", GridBoolHasheableObject(true), bytes, sizeof(bytes) / sizeof(*bytes), 1669973725);
        checkHash("bool - true-VAR", GridClientVariantHasheableObject(GridClientVariant(true)), bytes,
                sizeof(bytes) / sizeof(*bytes), 1669973725);
    }

    {
        int8_t bytes[] = { -43, 4, 0, 0 };

        checkHash("bool - false", GridBoolHasheableObject(false), bytes, sizeof(bytes) / sizeof(*bytes), -1900934144);

        bool valFalse = false;

        checkHash("bool - false-VAR", GridClientVariantHasheableObject(GridClientVariant(valFalse)), bytes,
                sizeof(bytes) / sizeof(*bytes), -1900934144);
    }

    {
        int8_t bytes[] = { 3, 0, 0, 0 };

        checkHash("3", GridInt32Hasheable(3), bytes, sizeof(bytes) / sizeof(*bytes), 386050343);
        checkHash("3-VAR", GridClientVariantHasheableObject(GridClientVariant(3)), bytes,
                sizeof(bytes) / sizeof(*bytes), 386050343);
    }

    {
        int8_t bytes[] = { 0, -54, -102, 59 };

        checkHash("1000000000", GridInt32Hasheable(1000000000), bytes, sizeof(bytes) / sizeof(*bytes),
                -547312286);
        checkHash("1000000000-VAR", GridClientVariantHasheableObject(GridClientVariant(1000000000)), bytes,
                sizeof(bytes) / sizeof(*bytes), -547312286);
    }

    {
        int8_t bytes[] = { -1, -1, -1, 127 };

        checkHash("0x7fffffff", GridInt32Hasheable(0x7fffffff), bytes, sizeof(bytes) / sizeof(*bytes),
                473949739);
        checkHash("0x7fffffff-VAR", GridClientVariantHasheableObject(GridClientVariant(0x7fffffff)), bytes,
                sizeof(bytes) / sizeof(*bytes), 473949739);
    }

    {
        int8_t bytes[] = { -1, -1, -1, -1 };

        checkHash("0xffffffff", GridInt32Hasheable(0xffffffff), bytes, sizeof(bytes) / sizeof(*bytes),
                -1399925094);
        checkHash("0xffffffff-VAR", GridClientVariantHasheableObject(GridClientVariant((int32_t) 0xffffffff)), bytes,
                sizeof(bytes) / sizeof(*bytes), -1399925094);
    }

    {
        int8_t bytes[] = { -1, -1, -1, -1, -1, -1, -1, 127 };

        checkHash("0x7fffffffffffffffL", GridInt64Hasheable(0x7fffffffffffffffL), bytes,
                sizeof(bytes) / sizeof(*bytes), 201097861);
        checkHash("0x7fffffffffffffffL-VAR",
                GridClientVariantHasheableObject(GridClientVariant((int64_t) 0x7fffffffffffffffL)), bytes,
                sizeof(bytes) / sizeof(*bytes), 201097861);
    }

    {
        int8_t bytes[] = { -1, -1, -1, -1, -1, -1, -1, -1 };

        checkHash("0xffffffffffffffffL", GridInt64Hasheable(0xffffffffffffffffL), bytes,
                sizeof(bytes) / sizeof(*bytes), -1484017934);
        checkHash("0xffffffffffffffffL-VAR",
                GridClientVariantHasheableObject(GridClientVariant((int64_t) 0xffffffffffffffffL)), bytes,
                sizeof(bytes) / sizeof(*bytes), -1484017934);
    }

    {
        int8_t bytes[] = { 1, 0, 0, 0 };

        checkHash("1.4e-45f", GridFloatHasheableObject(1.4e-45f), bytes, sizeof(bytes) / sizeof(*bytes), 1262722378);
        checkHash("1.4e-45f-VAR", GridClientVariantHasheableObject(GridClientVariant(1.4e-45f)), bytes,
                sizeof(bytes) / sizeof(*bytes), 1262722378);
    }

    {
        int8_t bytes[] = { 1, 0, 0, 0, 0, 0, 0, 0 };

        checkHash("4.9e-324", GridDoubleHasheableObject(4.9e-324), bytes, sizeof(bytes) / sizeof(*bytes), 1262722378);
        checkHash("4.9e-324-VAR", GridClientVariantHasheableObject(GridClientVariant(4.9e-324)), bytes,
                sizeof(bytes) / sizeof(*bytes), 1262722378);
    }

    {
        int8_t bytes[] = { -1, -1, -1, -1, -1, -1, -17, 127 };

        checkHash("1.7976931348623157e+308", GridDoubleHasheableObject(1.7976931348623157e+308), bytes,
                sizeof(bytes) / sizeof(*bytes), -783615357);
        checkHash("1.7976931348623157e+308-VAR",
                GridClientVariantHasheableObject(GridClientVariant(1.7976931348623157e+308)), bytes,
                sizeof(bytes) / sizeof(*bytes), -783615357);
    }

    checkUUID("224ea4cd-f449-4dcb-869a-5317c63bd619", 806670090);
    checkUUID("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd", -354375826);
    checkUUID("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636", -1312538272);
    checkUUID("d8f1f916-4357-4cfe-a7df-49d4721690bf", -482944041);
    checkUUID("d67eb652-4e76-47fb-ad4e-cd902d9b868a", -449444069);
    checkUUID("c77ffeae-78a1-4ee6-a0fd-8d197a794412", -168980875);
    checkUUID("35de9f21-3c9b-4f4a-a7d5-3e2c6cb01564", -383915637);
}

BOOST_AUTO_TEST_CASE(testCollisions) {
    std::map<int32_t, std::set<GridClientUuid> > map;

    std::set<GridClientUuid> nodes;

    int32_t iterCnt = 0;

    while (nodes.size() < 10) {
        iterCnt++;

        GridClientUuid id = GridClientUuid::randomUuid();

        int32_t hashCode = id.hashCode();

        std::set<GridClientUuid>& s = map[hashCode];

        s.insert(id);

        if (s.size() > 1)
            nodes.insert(s.begin(), s.end());
    }

    GG_LOG_INFO("Iterations count: %d", iterCnt);

    GridClientConsistentHashImpl hash;

    for (auto i = nodes.begin(); i != nodes.end(); i++)
        hash.addNode(NodeInfo(*i, std::shared_ptr<GridClientHasheableObject>(new GridClientUuid(*i))), 128);

    for (auto i = nodes.begin(); i != nodes.end(); i++) {
        std::set<NodeInfo> singleton;

        singleton.insert(NodeInfo(*i, std::shared_ptr<GridClientHasheableObject>(new GridClientUuid(*i))));

        GridClientUuid act = hash.node(GridInt32Hasheable(0), singleton).id();

        BOOST_CHECK_EQUAL(act, *i);
    }
}

typedef std::shared_ptr<GridClientHasheableObject> KeyT;
typedef std::map<KeyT, int32_t> DataMapT;

BOOST_AUTO_TEST_SUITE_END()

static void checkUUID(const string& uuid, int code) {
    checkHash(uuid.c_str(), GridClientUuid(uuid), 0, 0, code);
}

/**
 * Dumps bytes array into the console.
 *
 * @param bytes - std::vector<int8_t>&
 */
static void doDumpBytes(const std::vector<int8_t>& bytes) {
    std::stringstream os;

    os << "[";

    for (size_t i = 0; i < bytes.size(); ++i) {
        if (i > 0)
            os << ", ";

        //os << std::hex << ((short) bytes[i] & 0xff);
        os << ((int) (bytes[i]));
    }

    os << "]";

    cout << os.str();
}

/**
 * Compares two bytes array and dump each other if there is data mismatch.
 * @param testVal- const char* - The value to be tested.
 * @param hashBytes - std::vector<int8_t> - The real bytes array.
 * @param expectedBytes std::vector<int8_t> - The expected bytes array.
 *
 */
static void dumpHashObjectBytes(const char* testVal, const std::vector<int8_t>& hashBytes,
        const std::vector<int8_t>& expectedBytes) {
    if (!(hashBytes == expectedBytes)) {
        cout << "Mismatch in the value= " << testVal << "\n";

        doDumpBytes(hashBytes);

        doDumpBytes(expectedBytes);
    }
}

/**
 * Check hash generation for the specified object.
 *
 * @param o Object to verify hash generation for.
 * @param hash Expected bytes-array serialization of the object.
 * @param code Expected hash code.
 */
void checkHash(const char* testVal, const GridClientHasheableObject& o, const int8_t* bytes, size_t nBytes, int code) {
    std::vector<int8_t> hashBytes;

    if (bytes != NULL) {
        std::vector<int8_t> expectedBytes(bytes, bytes + nBytes);

        o.convertToBytes(hashBytes);

        dumpHashObjectBytes(testVal, hashBytes, expectedBytes);

        BOOST_CHECK(hashBytes == expectedBytes);
    }

    GridClientConsistentHashImpl hasher;

    int32_t hashCode = hasher.hash(o);

    BOOST_CHECK_EQUAL(code, hashCode);
}
