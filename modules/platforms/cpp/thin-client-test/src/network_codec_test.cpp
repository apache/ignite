/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <boost/test/unit_test.hpp>

#include <ignite/impl/interop/interop.h>
#include <ignite/network/length_prefix_codec.h>

using namespace boost::unit_test;
using namespace ignite::network;


BOOST_AUTO_TEST_SUITE(NetworkCodecTestSuite)

BOOST_AUTO_TEST_CASE(LengthPrefixCodec_3bytes)
{
    ignite::impl::interop::SP_InteropMemory mem(new ignite::impl::interop::InteropUnpooledMemory(8));
    ignite::impl::interop::InteropOutputStream stream(mem.Get());
    stream.WriteInt32(4);
    stream.WriteInt32(123456789);

    DataBuffer in1(mem, 0, 3);
    DataBuffer in2(mem, 3, 8);

    SP_Codec codec(new LengthPrefixCodec());

    DataBuffer out1 = codec.Get()->Decode(in1);
    BOOST_CHECK(out1.IsEmpty());

    DataBuffer out2 = codec.Get()->Decode(in2);
    BOOST_CHECK(!out2.IsEmpty());
    BOOST_CHECK_EQUAL(out2.GetSize(), 8);
}

BOOST_AUTO_TEST_SUITE_END()
