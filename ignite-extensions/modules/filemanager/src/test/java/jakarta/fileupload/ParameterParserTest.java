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
package jakarta.fileupload;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for {@link ParameterParser}.
 */
public class ParameterParserTest {

    @Test
    public void testParsing() {
        String s =
            "test; test1 =  stuff   ; test2 =  \"stuff; stuff\"; test3=\"stuff";
        ParameterParser parser = new ParameterParser();
        Map<String, String> params = parser.parse(s, ';');
        assertEquals(null, params.get("test"));
        assertEquals("stuff", params.get("test1"));
        assertEquals("stuff; stuff", params.get("test2"));
        assertEquals("\"stuff", params.get("test3"));

        params = parser.parse(s, new char[] { ',', ';' });
        assertEquals(null, params.get("test"));
        assertEquals("stuff", params.get("test1"));
        assertEquals("stuff; stuff", params.get("test2"));
        assertEquals("\"stuff", params.get("test3"));

        s = "  test  , test1=stuff   ,  , test2=, test3, ";
        params = parser.parse(s, ',');
        assertEquals(null, params.get("test"));
        assertEquals("stuff", params.get("test1"));
        assertEquals(null, params.get("test2"));
        assertEquals(null, params.get("test3"));

        s = "  test";
        params = parser.parse(s, ';');
        assertEquals(null, params.get("test"));

        s = "  ";
        params = parser.parse(s, ';');
        assertEquals(0, params.size());

        s = " = stuff ";
        params = parser.parse(s, ';');
        assertEquals(0, params.size());
    }

    @Test
    public void testContentTypeParsing() {
        String s = "text/plain; Charset=UTF-8";
        ParameterParser parser = new ParameterParser();
        parser.setLowerCaseNames(true);
        Map<String, String> params = parser.parse(s, ';');
        assertEquals("UTF-8", params.get("charset"));
    }

    @Test
    public void testParsingEscapedChars() {
        String s = "param = \"stuff\\\"; more stuff\"";
        ParameterParser parser = new ParameterParser();
        Map<String, String> params = parser.parse(s, ';');
        assertEquals(1, params.size());
        assertEquals("stuff\\\"; more stuff", params.get("param"));

        s = "param = \"stuff\\\\\"; anotherparam";
        params = parser.parse(s, ';');
        assertEquals(2, params.size());
        assertEquals("stuff\\\\", params.get("param"));
        assertNull(params.get("anotherparam"));
    }

    // See: http://issues.apache.org/jira/browse/FILEUPLOAD-139
    @Test
    public void testFileUpload139() {
        ParameterParser parser = new ParameterParser();
        String s = "Content-type: multipart/form-data , boundary=AaB03x";
        Map<String, String> params = parser.parse(s, new char[] { ',', ';' });
        assertEquals("AaB03x", params.get("boundary"));

        s = "Content-type: multipart/form-data, boundary=AaB03x";
        params = parser.parse(s, new char[] { ';', ',' });
        assertEquals("AaB03x", params.get("boundary"));

        s = "Content-type: multipart/mixed, boundary=BbC04y";
        params = parser.parse(s, new char[] { ',', ';' });
        assertEquals("BbC04y", params.get("boundary"));
    }

    /**
     * Test for <a href="http://issues.apache.org/jira/browse/FILEUPLOAD-199">FILEUPLOAD-199</a>
     */
    @Test
    public void fileUpload199() {
        ParameterParser parser = new ParameterParser();
        String s = "Content-Disposition: form-data; name=\"file\"; filename=\"=?ISO-8859-1?B?SWYgeW91IGNhbiByZWFkIHRoaXMgeW8=?= =?ISO-8859-2?B?dSB1bmRlcnN0YW5kIHRoZSBleGFtcGxlLg==?=\"\r\n";
        Map<String, String> params = parser.parse(s, new char[] { ',', ';' });
        assertEquals("If you can read this you understand the example.", params.get("filename"));
    }

}
