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

final class HttpServletRequestFactory {

    static public HttpServletRequest createHttpServletRequestWithNullContentType() {
        byte[] requestData = "foobar".getBytes();
        return new MockHttpServletRequest(
                            requestData,
                            null);
    }

    static public HttpServletRequest createValidHttpServletRequest(
            final String[] strFileNames) {
        // todo - provide a real implementation

        StringBuilder sbRequestData = new StringBuilder();

        for (String strFileName : strFileNames) {
            sbRequestData.append(strFileName);
        }

        byte[] requestData = null;
        requestData = sbRequestData.toString().getBytes();

        return new MockHttpServletRequest(
                            requestData,
                            FileUploadBase.MULTIPART_FORM_DATA);
    }

    static public HttpServletRequest createInvalidHttpServletRequest() {
        byte[] requestData = "foobar".getBytes();
        return new MockHttpServletRequest(
                            requestData,
                            FileUploadBase.MULTIPART_FORM_DATA);
    }

}
