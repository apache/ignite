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

package org.apache.ignite.internal.processors.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

import static org.apache.ignite.internal.processors.security.impl.TestSslSecurityPluginProvider.
    ATTR_SECURITY_CERTIFICATES;

/**
 * Creates user attributes for thin clients.
 */
public abstract class SslAbstractNodeAttributesFactory implements Factory<Map<String, Object>> {
    /** JKS path. */
    protected String jksPath;

    /** Certificate alias. */
    protected String alias;

    /** Pass. */
    protected char[] pass = "123456".toCharArray();

    @Override public Map<String, Object> create() {
        try {
            KeyStore clientKeyStore = KeyStore.getInstance("JKS");
            File file = new File(jksPath);

            if (!file.exists())
                return null;

            try (InputStream ksInputStream = new FileInputStream(file)) {
                clientKeyStore.load(ksInputStream, pass);

                Certificate[] certs = clientKeyStore.getCertificateChain(alias);
                JdkMarshaller marshaller = MarshallerUtils.jdkMarshaller(null);

                HashMap<String, Object> map = new HashMap<>();

                map.put(ATTR_SECURITY_CERTIFICATES, U.marshal(marshaller, certs));

                return map;
            }
        }
        catch (Exception e) {
            return null;
        }
    }
}
