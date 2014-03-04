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

#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>

#include <boost/test/unit_test.hpp>

#include "gridgain/impl/marshaller/protobuf/ClientMessages.pb.h"

BOOST_AUTO_TEST_SUITE(GridClientProtobufSerializationSimpleTest)

using namespace std;
using namespace org::gridgain::grid::kernal::processors::rest::client::message;

BOOST_AUTO_TEST_CASE(protoBuf) {
    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    KeyValue keyValue, keyValue1;
    ObjectWrapper ow, ow1;

    ow.set_type(STRING);
    ow.set_binary("Ivan");

    ow1.set_type(STRING);
    ow1.set_binary("Petrov");

    keyValue.mutable_key()->CopyFrom(ow);
    keyValue.mutable_value()->CopyFrom(ow1);

    const char* tmpfile;

#ifdef _MSC_VER
    std::auto_ptr<char> tmpfileptr;
    tmpfileptr.reset(_tempnam( "c:\\tmp", "tmp"));
    tmpfile=tmpfileptr.get();
#else
    tmpfile=tmpnam(NULL);
#endif

    fstream output(tmpfile, ios::out | ios::trunc | ios::binary);

    if (!keyValue.SerializeToOstream(&output)) {
        cerr << "Failed to write key and value.";

		BOOST_CHECK(false);
    }

    output.close();

    fstream input(tmpfile, ios::in | ios::binary);

    if (!keyValue1.ParseFromIstream(&input)) {
        cerr << "Failed to parse data.";

        BOOST_CHECK(false);

		return;
    }

    input.close();

    BOOST_CHECK(keyValue1.has_key());
    BOOST_CHECK(keyValue1.has_value());

    BOOST_CHECK(keyValue.key().binary() == keyValue1.key().binary());
    BOOST_CHECK(keyValue.value().binary() == keyValue1.value().binary());
}

BOOST_AUTO_TEST_SUITE_END()
