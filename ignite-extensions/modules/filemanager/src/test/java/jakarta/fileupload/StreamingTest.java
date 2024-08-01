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
import junit.framework.TestCase;
import jakarta.fileupload.FileUploadBase.IOFileUploadException;
import jakarta.fileupload.disk.DiskFileItemFactory;
import jakarta.fileupload.servlet.ServletFileUpload;
import jakarta.fileupload.servlet.ServletRequestContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.List;

/**
 * Unit test for items with varying sizes.
 */
public class StreamingTest extends TestCase {

    /**
     * Tests a file upload with varying file sizes.
     */
    public void testFileUpload()
            throws IOException, FileUploadException {
        byte[] request = newRequest();
        List<FileItem> fileItems = parseUpload(request);
        Iterator<FileItem> fileIter = fileItems.iterator();
        int add = 16;
        int num = 0;
        for (int i = 0;  i < 16384;  i += add) {
            if (++add == 32) {
                add = 16;
            }
            FileItem item = fileIter.next();
            assertEquals("field" + (num++), item.getFieldName());
            byte[] bytes = item.get();
            assertEquals(i, bytes.length);
            for (int j = 0;  j < i;  j++) {
                assertEquals((byte) j, bytes[j]);
            }
        }
        assertTrue(!fileIter.hasNext());
    }

    /**
     * Tests, whether an invalid request throws a proper
     * exception.
     */
    public void testFileUploadException()
            throws IOException, FileUploadException {
        byte[] request = newRequest();
        byte[] invalidRequest = new byte[request.length-11];
        System.arraycopy(request, 0, invalidRequest, 0, request.length-11);
        try {
            parseUpload(invalidRequest);
            fail("Expected EndOfStreamException");
        } catch (IOFileUploadException e) {
            assertTrue(e.getCause() instanceof MultipartStream.MalformedStreamException);
        }
    }

    /**
     * Tests, whether an IOException is properly delegated.
     */
    public void testIOException()
            throws IOException {
        byte[] request = newRequest();
        InputStream stream = new FilterInputStream(new ByteArrayInputStream(request)){
            private int num;
            @Override
            public int read() throws IOException {
                if (++num > 123) {
                    throw new IOException("123");
                }
                return super.read();
            }
            @Override
            public int read(byte[] pB, int pOff, int pLen)
                    throws IOException {
                for (int i = 0;  i < pLen;  i++) {
                    int res = read();
                    if (res == -1) {
                        return i == 0 ? -1 : i;
                    }
                    pB[pOff+i] = (byte) res;
                }
                return pLen;
            }
        };
        try {
            parseUpload(stream, request.length);
            fail("Expected IOException");
        } catch (FileUploadException e) {
            assertTrue(e.getCause() instanceof IOException);
            assertEquals("123", e.getCause().getMessage());
        }
    }

    /**
     * Test for FILEUPLOAD-135
     */
    public void testFILEUPLOAD135()
            throws IOException, FileUploadException {
        byte[] request = newShortRequest();
        final ByteArrayInputStream bais = new ByteArrayInputStream(request);
        List<FileItem> fileItems = parseUpload(new InputStream() {
            @Override
            public int read()
            throws IOException
            {
                return bais.read();
            }
            @Override
            public int read(byte b[], int off, int len) throws IOException
            {
                return bais.read(b, off, Math.min(len, 3));
            }

        }, request.length);
        Iterator<FileItem> fileIter = fileItems.iterator();
        assertTrue(fileIter.hasNext());
        FileItem item = fileIter.next();
        assertEquals("field", item.getFieldName());
        byte[] bytes = item.get();
        assertEquals(3, bytes.length);
        assertEquals((byte)'1', bytes[0]);
        assertEquals((byte)'2', bytes[1]);
        assertEquals((byte)'3', bytes[2]);
        assertTrue(!fileIter.hasNext());
    }

    private List<FileItem> parseUpload(byte[] bytes) throws FileUploadException {
        return parseUpload(new ByteArrayInputStream(bytes), bytes.length);
    }

    private FileItemIterator parseUpload(int pLength, InputStream pStream)
            throws FileUploadException, IOException {
        String contentType = "multipart/form-data; boundary=---1234";

        FileUploadBase upload = new ServletFileUpload();
        upload.setFileItemFactory(new DiskFileItemFactory());
        HttpServletRequest request = new MockHttpServletRequest(pStream,
                pLength, contentType);

        return upload.getItemIterator(new ServletRequestContext(request));
    }

    private List<FileItem> parseUpload(InputStream pStream, int pLength)
            throws FileUploadException {
        String contentType = "multipart/form-data; boundary=---1234";

        FileUploadBase upload = new ServletFileUpload();
        upload.setFileItemFactory(new DiskFileItemFactory());
        HttpServletRequest request = new MockHttpServletRequest(pStream,
                pLength, contentType);

        List<FileItem> fileItems = upload.parseRequest(new ServletRequestContext(request));
        return fileItems;
    }

    private String getHeader(String pField) {
        return "-----1234\r\n"
            + "Content-Disposition: form-data; name=\"" + pField + "\"\r\n"
            + "\r\n";

    }

    private String getFooter() {
        return "-----1234--\r\n";
    }

    private byte[] newShortRequest() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final OutputStreamWriter osw = new OutputStreamWriter(baos, "US-ASCII");
        osw.write(getHeader("field"));
        osw.write("123");
        osw.write("\r\n");
        osw.write(getFooter());
        osw.close();
        return baos.toByteArray();
    }

    private byte[] newRequest() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final OutputStreamWriter osw = new OutputStreamWriter(baos, "US-ASCII");
        int add = 16;
        int num = 0;
        for (int i = 0;  i < 16384;  i += add) {
            if (++add == 32) {
                add = 16;
            }
            osw.write(getHeader("field" + (num++)));
            osw.flush();
            for (int j = 0;  j < i;  j++) {
                baos.write((byte) j);
            }
            osw.write("\r\n");
        }
        osw.write(getFooter());
        osw.close();
        return baos.toByteArray();
    }

    /**
     * Tests, whether an {@link InvalidFileNameException} is thrown.
     */
    public void testInvalidFileNameException() throws Exception {
        final String fileName = "foo.exe\u0000.png";
        final String request =
            "-----1234\r\n" +
            "Content-Disposition: form-data; name=\"file\"; filename=\"" + fileName + "\"\r\n" +
            "Content-Type: text/whatever\r\n" +
            "\r\n" +
            "This is the content of the file\n" +
            "\r\n" +
            "-----1234\r\n" +
            "Content-Disposition: form-data; name=\"field\"\r\n" +
            "\r\n" +
            "fieldValue\r\n" +
            "-----1234\r\n" +
            "Content-Disposition: form-data; name=\"multi\"\r\n" +
            "\r\n" +
            "value1\r\n" +
            "-----1234\r\n" +
            "Content-Disposition: form-data; name=\"multi\"\r\n" +
            "\r\n" +
            "value2\r\n" +
            "-----1234--\r\n";
        final byte[] reqBytes = request.getBytes("US-ASCII");

        FileItemIterator fileItemIter = parseUpload(reqBytes.length, new ByteArrayInputStream(reqBytes));
        final FileItemStream fileItemStream = fileItemIter.next();
        try {
            fileItemStream.getName();
            fail("Expected exception");
        } catch (InvalidFileNameException e) {
            assertEquals(fileName, e.getName());
            assertTrue(e.getMessage().indexOf(fileName) == -1);
            assertTrue(e.getMessage().indexOf("foo.exe\\0.png") != -1);
        }

        List<FileItem> fileItems = parseUpload(reqBytes);
        final FileItem fileItem = fileItems.get(0);
        try {
            fileItem.getName();
            fail("Expected exception");
        } catch (InvalidFileNameException e) {
            assertEquals(fileName, e.getName());
            assertTrue(e.getMessage().indexOf(fileName) == -1);
            assertTrue(e.getMessage().indexOf("foo.exe\\0.png") != -1);
        }
    }

}
