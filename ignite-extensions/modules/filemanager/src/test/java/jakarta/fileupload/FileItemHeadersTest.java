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

import jakarta.fileupload.util.FileItemHeadersImpl;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests {@link FileItemHeaders} and
 * {@link FileItemHeadersImpl}.
 */
public class FileItemHeadersTest {

    /**
     * @throws Exception
     */
    @Test
    public void testFileItemHeaders() throws Exception {
        FileItemHeadersImpl aMutableFileItemHeaders = new FileItemHeadersImpl();
        aMutableFileItemHeaders.addHeader("Content-Disposition", "form-data; name=\"FileItem\"; filename=\"file1.txt\"");
        aMutableFileItemHeaders.addHeader("Content-Type", "text/plain");

        aMutableFileItemHeaders.addHeader("TestHeader", "headerValue1");
        aMutableFileItemHeaders.addHeader("TestHeader", "headerValue2");
        aMutableFileItemHeaders.addHeader("TestHeader", "headerValue3");
        aMutableFileItemHeaders.addHeader("testheader", "headerValue4");

        Iterator<String> headerNameEnumeration = aMutableFileItemHeaders.getHeaderNames();
        assertEquals("content-disposition", headerNameEnumeration.next());
        assertEquals("content-type", headerNameEnumeration.next());
        assertEquals("testheader", headerNameEnumeration.next());
        assertFalse(headerNameEnumeration.hasNext());

        assertEquals(aMutableFileItemHeaders.getHeader("Content-Disposition"), "form-data; name=\"FileItem\"; filename=\"file1.txt\"");
        assertEquals(aMutableFileItemHeaders.getHeader("Content-Type"), "text/plain");
        assertEquals(aMutableFileItemHeaders.getHeader("content-type"), "text/plain");
        assertEquals(aMutableFileItemHeaders.getHeader("TestHeader"), "headerValue1");
        assertNull(aMutableFileItemHeaders.getHeader("DummyHeader"));

        Iterator<String> headerValueEnumeration;

        headerValueEnumeration = aMutableFileItemHeaders.getHeaders("Content-Type");
        assertTrue(headerValueEnumeration.hasNext());
        assertEquals(headerValueEnumeration.next(), "text/plain");
        assertFalse(headerValueEnumeration.hasNext());

        headerValueEnumeration = aMutableFileItemHeaders.getHeaders("content-type");
        assertTrue(headerValueEnumeration.hasNext());
        assertEquals(headerValueEnumeration.next(), "text/plain");
        assertFalse(headerValueEnumeration.hasNext());

        headerValueEnumeration = aMutableFileItemHeaders.getHeaders("TestHeader");
        assertTrue(headerValueEnumeration.hasNext());
        assertEquals(headerValueEnumeration.next(), "headerValue1");
        assertTrue(headerValueEnumeration.hasNext());
        assertEquals(headerValueEnumeration.next(), "headerValue2");
        assertTrue(headerValueEnumeration.hasNext());
        assertEquals(headerValueEnumeration.next(), "headerValue3");
        assertTrue(headerValueEnumeration.hasNext());
        assertEquals(headerValueEnumeration.next(), "headerValue4");
        assertFalse(headerValueEnumeration.hasNext());

        headerValueEnumeration = aMutableFileItemHeaders.getHeaders("DummyHeader");
        assertFalse(headerValueEnumeration.hasNext());
    }

}
