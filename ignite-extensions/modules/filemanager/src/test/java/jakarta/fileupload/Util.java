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
import jakarta.fileupload.disk.DiskFileItemFactory;

import jakarta.fileupload.servlet.ServletFileUpload;
import jakarta.fileupload.servlet.ServletRequestContext;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

/**
 * Test utility methods.
 *
 * @since 1.4
 */
public class Util {

    public static List<FileItem> parseUpload(FileUpload upload, byte[] bytes) throws FileUploadException {
        return parseUpload(upload, bytes, Constants.CONTENT_TYPE);
    }

    public static List<FileItem> parseUpload(FileUpload upload, byte[] bytes, String contentType) throws FileUploadException {
        final HttpServletRequest request = new MockHttpServletRequest(bytes, contentType);
        List<FileItem> fileItems = upload.parseRequest(new ServletRequestContext(request));
        return fileItems;
    }

    public static List<FileItem> parseUpload(FileUpload upload, String content)
        throws UnsupportedEncodingException, FileUploadException {
        byte[] bytes = content.getBytes("US-ASCII");
        return parseUpload(upload, bytes, Constants.CONTENT_TYPE);
    }

    /**
     * Return a list of {@link FileUpload} implementations for parameterized tests.
     * @return a list of {@link FileUpload} implementations
     */
    public static List<FileUpload> fileUploadImplementations() {
        return Arrays.asList(
                new ServletFileUpload(new DiskFileItemFactory())
                );
    }
}
