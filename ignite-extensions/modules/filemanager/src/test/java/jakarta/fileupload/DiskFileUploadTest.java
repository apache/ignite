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

import javax.servlet.http.HttpServletRequest;
import jakarta.fileupload.disk.DiskFileItem;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test for {@link DiskFileUpload}. Remove when deprecated class is removed.
 *
 * @since 1.4
 */
@SuppressWarnings({"deprecation"}) // unit tests for deprecated class
public class DiskFileUploadTest {

    private DiskFileUpload upload;

    @Before
    public void setUp() {
        upload = new DiskFileUpload();
    }

    @Test
    public void testWithInvalidRequest() {
        HttpServletRequest req = HttpServletRequestFactory.createInvalidHttpServletRequest();

        try {
            upload.parseRequest(req);
            fail("testWithInvalidRequest: expected exception was not thrown");
        } catch (FileUploadException expected) {
            // this exception is expected
        }
    }

    @Test
    public void testWithNullContentType() {
        HttpServletRequest req = HttpServletRequestFactory.createHttpServletRequestWithNullContentType();

        try {
            upload.parseRequest(req);
            fail("testWithNullContentType: expected exception was not thrown");
        } catch (DiskFileUpload.InvalidContentTypeException expected) {
            // this exception is expected
        } catch (FileUploadException unexpected) {
            fail("testWithNullContentType: unexpected exception was thrown");
        }
    }

    /** Proposed test for FILEUPLOAD-293. As of yet, doesn't reproduce the problem.
     */
    @Test
    public void testMoveFile() throws Exception {
        DiskFileUpload myUpload = new DiskFileUpload();
        myUpload.setSizeThreshold(0);
    	final String content =
                "-----1234\r\n" +
                "Content-Disposition: form-data; name=\"file\";"
                		+ "filename=\"foo.tab\"\r\n" +
                "Content-Type: text/whatever\r\n" +
                "\r\n" +
                "This is the content of the file\n" +
                "\r\n" +
                "-----1234--\r\n";
    	final byte[] contentBytes = content.getBytes("US-ASCII");
        final HttpServletRequest request = new MockHttpServletRequest(contentBytes, Constants.CONTENT_TYPE);
        final List<FileItem> items = myUpload.parseRequest(request);
        assertNotNull(items);
        assertFalse(items.isEmpty());
        final DiskFileItem dfi = (DiskFileItem) items.get(0);
        final File out = File.createTempFile("install", ".tmp");
        dfi.write(out);
    }
}
