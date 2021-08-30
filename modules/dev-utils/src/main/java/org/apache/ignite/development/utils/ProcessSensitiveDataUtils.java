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

package org.apache.ignite.development.utils;

import java.security.MessageDigest;
import java.util.Base64;
import org.apache.ignite.IgniteException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utility class for processing sensitive data.
 */
class ProcessSensitiveDataUtils {
    /**
     * Conversion to md5 hash string.
     *
     * @param val String value.
     * @return MD5 hash string.
     * */
    public static String md5(String val) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(val.getBytes(UTF_8));

            byte[] digest = md.digest();

            return Base64.getEncoder().encodeToString(digest);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Private constructor.
     */
    private ProcessSensitiveDataUtils() {
        throw new RuntimeException("Don't create.");
    }
}
