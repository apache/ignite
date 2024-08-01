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

import jakarta.fileupload.servlet.ServletFileUpload;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the {@link ProgressListener}.
 */
public class ProgressListenerTest {

    private class ProgressListenerImpl implements ProgressListener {

        private final long expectedContentLength;

        private final int expectedItems;

        private Long bytesRead;

        private Integer items;

        ProgressListenerImpl(long pContentLength, int pItems) {
            expectedContentLength = pContentLength;
            expectedItems = pItems;
        }

        @Override
        public void update(long pBytesRead, long pContentLength, int pItems) {
            assertTrue(pBytesRead >= 0  &&  pBytesRead <= expectedContentLength);
            assertTrue(pContentLength == -1  ||  pContentLength == expectedContentLength);
            assertTrue(pItems >= 0  &&  pItems <= expectedItems);

            assertTrue(bytesRead == null  ||  pBytesRead >= bytesRead.longValue());
            bytesRead = new Long(pBytesRead);
            assertTrue(items == null  ||  pItems >= items.intValue());
            items = new Integer(pItems);
        }

        void checkFinished(){
            assertEquals(expectedContentLength, bytesRead.longValue());
            assertEquals(expectedItems, items.intValue());
        }

    }

    /**
     * Parse a very long file upload by using a progress listener.
     */
    @Test
    public void testProgressListener() throws Exception {
        final int NUM_ITEMS = 512;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (int i = 0;  i < NUM_ITEMS;  i++) {
            String header = "-----1234\r\n"
                + "Content-Disposition: form-data; name=\"field" + (i+1) + "\"\r\n"
                + "\r\n";
            baos.write(header.getBytes("US-ASCII"));
            for (int j = 0;  j < 16384+i;  j++) {
                baos.write((byte) j);
            }
            baos.write("\r\n".getBytes("US-ASCII"));
        }
        baos.write("-----1234--\r\n".getBytes("US-ASCII"));
        byte[] contents = baos.toByteArray();

        MockHttpServletRequest request = new MockHttpServletRequest(contents, Constants.CONTENT_TYPE);
        runTest(NUM_ITEMS, contents.length, request);
        request = new MockHttpServletRequest(contents, Constants.CONTENT_TYPE){
            @Override
            public int getContentLength() {
                return -1;
            }
        };
        runTest(NUM_ITEMS, contents.length, request);
    }

    private void runTest(final int NUM_ITEMS, long pContentLength, MockHttpServletRequest request) throws FileUploadException, IOException {
        ServletFileUpload upload = new ServletFileUpload();
        ProgressListenerImpl listener = new ProgressListenerImpl(pContentLength, NUM_ITEMS);
        upload.setProgressListener(listener);
        FileItemIterator iter = upload.getItemIterator(request);
        for (int i = 0;  i < NUM_ITEMS;  i++) {
            FileItemStream stream = iter.next();
            InputStream istream = stream.openStream();
            for (int j = 0;  j < 16384+i;  j++) {
                /**
                 * This used to be
                 *     assertEquals((byte) j, (byte) istream.read());
                 * but this seems to trigger a bug in JRockit, so
                 * we express the same like this:
                 */
                byte b1 = (byte) j;
                byte b2 = (byte) istream.read();
                if (b1 != b2) {
                    fail("Expected " + b1 + ", got " + b2);
                }
            }
            assertEquals(-1, istream.read());
            istream.close();
        }
        assertTrue(!iter.hasNext());
        listener.checkFinished();
    }

}
