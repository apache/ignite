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

#include <boost/test/unit_test.hpp>

#include <gridgain/gridgain.hpp>

BOOST_AUTO_TEST_SUITE(GridClientHashCodeSuite)

BOOST_AUTO_TEST_CASE(testHashMethods) {
    BOOST_CHECK_EQUAL(1237, gridBoolHash(false));

    BOOST_CHECK_EQUAL(1231, gridBoolHash(true));

    BOOST_CHECK_EQUAL(10, gridByteHash(10));

    BOOST_CHECK_EQUAL(-10, gridByteHash(-10));

    BOOST_CHECK_EQUAL(10, gridInt16Hash(10));

    BOOST_CHECK_EQUAL(-10, gridInt16Hash(-10));

    BOOST_CHECK_EQUAL(10, gridInt32Hash(10));

    BOOST_CHECK_EQUAL(-10, gridInt32Hash(-10));

    BOOST_CHECK_EQUAL(10, gridInt64Hash(10));

    BOOST_CHECK_EQUAL(9, gridInt64Hash(-10));

    BOOST_CHECK_EQUAL(1093140480, gridFloatHash(10.5f));

    BOOST_CHECK_EQUAL(-1054343168, gridFloatHash(-10.5f));

    BOOST_CHECK_EQUAL(1076166656, gridDoubleHash(10.5f));

    BOOST_CHECK_EQUAL(-1071316992, gridDoubleHash(-10.5f));

    BOOST_CHECK_EQUAL(-738730625, GridClientDate(1403715574841).hashCode());
}

BOOST_AUTO_TEST_CASE(testVariantHash) {
    TGridClientVariantMap map;

    map[GridClientVariant(1)] = GridClientVariant(2);
    map[GridClientVariant(3)] = GridClientVariant(4);
    map[GridClientVariant(3.5f)] = GridClientVariant(-10.5);

    BOOST_CHECK_EQUAL(-2142961654, GridClientVariant(map).hashCode());
}

BOOST_AUTO_TEST_SUITE_END()
