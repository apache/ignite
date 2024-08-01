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
package jakarta.fileupload.util.mime;

import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;

/**
 * Use the online <a href="http://dogmamix.com/MimeHeadersDecoder/">MimeHeadersDecoder</a>
 * to validate expected values.
 *
 * @since 1.3
 */
public final class MimeUtilityTestCase {

    @Test
    public void noNeedToDecode() throws Exception {
        assertEncoded("abc", "abc");
    }

    @Test
    public void decodeUtf8QuotedPrintableEncoded() throws Exception {
        assertEncoded(" h\u00e9! \u00e0\u00e8\u00f4u !!!", "=?UTF-8?Q?_h=C3=A9!_=C3=A0=C3=A8=C3=B4u_!!!?=");
    }

    @Test
    public void decodeUtf8Base64Encoded() throws Exception {
        assertEncoded(" h\u00e9! \u00e0\u00e8\u00f4u !!!", "=?UTF-8?B?IGjDqSEgw6DDqMO0dSAhISE=?=");
    }

    @Test
    public void decodeIso88591Base64Encoded() throws Exception {
        assertEncoded("If you can read this you understand the example.",
                      "=?ISO-8859-1?B?SWYgeW91IGNhbiByZWFkIHRoaXMgeW8=?= =?ISO-8859-2?B?dSB1bmRlcnN0YW5kIHRoZSBleGFtcGxlLg==?=\"\r\n");
    }

    @Test
    public void decodeIso88591Base64EncodedWithWhiteSpace() throws Exception {
        assertEncoded("If you can read this you understand the example.",
                "=?ISO-8859-1?B?SWYgeW91IGNhbiByZWFkIHRoaXMgeW8=?=\t  \r\n   =?ISO-8859-2?B?dSB1bmRlcnN0YW5kIHRoZSBleGFtcGxlLg==?=\"\r\n");
    }

    private static void assertEncoded(String expected, String encoded) throws Exception {
        assertEquals(expected, MimeUtility.decodeText(encoded));
    }

    @Test(expected=UnsupportedEncodingException.class)
    public void decodeInvalidEncoding() throws Exception {
        MimeUtility.decodeText("=?invalid?B?xyz-?=");
    }
}
