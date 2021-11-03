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

package org.apache.ignite.network;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link NetworkAddress}.
 */
class NetworkAddressTest {
    /**
     * Test parsing of a {@link NetworkAddress} from a string.
     */
    @Test
    void testFromString() {
        var addr = NetworkAddress.from("foobar:1234");

        assertThat(addr.host(), is("foobar"));
        assertThat(addr.port(), is(1234));
    }

    /**
     * Test parsing of a {@link NetworkAddress} from a string that contains a colon, to test for unexpected errors.
     */
    @Test
    void testFromStringWithColon() {
        var addr = NetworkAddress.from("foo:bar:1234");

        assertThat(addr.host(), is("foo:bar"));
        assertThat(addr.port(), is(1234));
    }

    /**
     * Test parsing of a {@link NetworkAddress} from a string that does not follow the required format.
     */
    @Test
    void testFromMalformedString() {
        assertThrows(IllegalArgumentException.class, () -> NetworkAddress.from("abcdef"));
    }

    /**
     * Test parsing of a {@link NetworkAddress} from a string that does not contain a numeric port.
     */
    @Test
    void testFromMalformedPortString() {
        assertThrows(IllegalArgumentException.class, () -> NetworkAddress.from("foobar:baz"));
    }

    /**
     * Test parsing of a {@link NetworkAddress} from an {@link InetSocketAddress}.
     */
    @Test
    void testFromInetAddr() {
        var inetAddr = InetSocketAddress.createUnresolved("foobar", 1234);
        var addr = NetworkAddress.from(inetAddr);

        assertThat(addr.host(), is("foobar"));
        assertThat(addr.port(), is(1234));
    }
}
