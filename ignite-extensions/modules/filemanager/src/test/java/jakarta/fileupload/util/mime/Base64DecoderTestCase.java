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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @since 1.3
 */
public final class Base64DecoderTestCase {

    private static final String US_ASCII_CHARSET = "US-ASCII";

    /**
     * Tests RFC 4648 section 10 test vectors.
     * <ul>
     * <li>BASE64("") = ""</li>
     * <li>BASE64("f") = "Zg=="</li>
     * <li>BASE64("fo") = "Zm8="</li>
     * <li>BASE64("foo") = "Zm9v"</li>
     * <li>BASE64("foob") = "Zm9vYg=="</li>
     * <li>BASE64("fooba") = "Zm9vYmE="</li>
     * <li>BASE64("foobar") = "Zm9vYmFy"</li>
     * </ul>
     *
     * @see <a href="http://tools.ietf.org/html/rfc4648">http://tools.ietf.org/html/rfc4648</a>
     */
    @Test
    public void rfc4648Section10Decode() throws Exception {
        assertEncoded("", "");
        assertEncoded("f", "Zg==");
        assertEncoded("fo", "Zm8=");
        assertEncoded("foo", "Zm9v");
        assertEncoded("foob", "Zm9vYg==");
        assertEncoded("fooba", "Zm9vYmE=");
        assertEncoded("foobar", "Zm9vYmFy");
    }

    /**
     * Test our decode with pad character in the middle.
     * Continues provided that the padding is in the correct place,
     * i.e. concatenated valid strings decode OK.
     */
    @Test
    public void decodeWithInnerPad() throws Exception {
        assertEncoded("Hello WorldHello World", "SGVsbG8gV29ybGQ=SGVsbG8gV29ybGQ=");
    }

    /**
     * Ignores non-BASE64 bytes.
     */
    @Test
    public void nonBase64Bytes() throws Exception {
        assertEncoded("Hello World", "S?G!V%sbG 8g\rV\t\n29ybGQ*=");
    }

    @Test(expected = IOException.class)
    public void truncatedString() throws Exception {
        final byte[] x = new byte[]{'n'};
        Base64Decoder.decode(x, new ByteArrayOutputStream());
    }

    @Test
    public void decodeTrailingJunk() throws Exception {
        assertEncoded("foobar", "Zm9vYmFy!!!");
    }

    // If there are valid trailing Base64 chars, complain
    @Test
    public void decodeTrailing1() throws Exception {
        assertIOException("truncated", "Zm9vYmFy1");
    }

    // If there are valid trailing Base64 chars, complain
    @Test
    public void decodeTrailing2() throws Exception {
        assertIOException("truncated", "Zm9vYmFy12");
    }

    // If there are valid trailing Base64 chars, complain
    @Test
    public void decodeTrailing3() throws Exception {
        assertIOException("truncated", "Zm9vYmFy123");
    }

    @Test
    public void badPadding() throws Exception {
        assertIOException("incorrect padding, 4th byte", "Zg=a");
    }

    @Test
    public void badPaddingLeading1() throws Exception {
        assertIOException("incorrect padding, first two bytes cannot be padding", "=A==");
    }

    @Test
    public void badPaddingLeading2() throws Exception {
        assertIOException("incorrect padding, first two bytes cannot be padding", "====");
    }

    // This input causes java.lang.ArrayIndexOutOfBoundsException: 1
    // in the Java 6 method DatatypeConverter.parseBase64Binary(String)
    // currently reported as truncated (the last chunk consists just of '=')
    @Test
    public void badLength() throws Exception {
        assertIOException("truncated", "Zm8==");
    }

    // These inputs cause java.lang.ArrayIndexOutOfBoundsException
    // in the Java 6 method DatatypeConverter.parseBase64Binary(String)
    // The non-ASCII characters should just be ignored.
    @Test
    public void nonASCIIcharacter() throws Exception {
        assertEncoded("f","Zg=�="); // A-grave
        assertEncoded("f","Zg=\u0100=");
    }

    private static void assertEncoded(String clearText, String encoded) throws Exception {
        byte[] expected = clearText.getBytes(US_ASCII_CHARSET);

        ByteArrayOutputStream out = new ByteArrayOutputStream(encoded.length());
        byte[] encodedData = encoded.getBytes(US_ASCII_CHARSET);
        Base64Decoder.decode(encodedData, out);
        byte[] actual = out.toByteArray();

        assertArrayEquals(expected, actual);
    }

    private static void assertIOException(String messageText, String encoded) throws UnsupportedEncodingException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(encoded.length());
        byte[] encodedData = encoded.getBytes(US_ASCII_CHARSET);
        try {
            Base64Decoder.decode(encodedData, out);
            fail("Expected IOException");
        } catch (IOException e) {
            String em = e.getMessage();
            assertTrue("Expected to find " + messageText + " in '" + em + "'",em.contains(messageText));
        }
    }

}
