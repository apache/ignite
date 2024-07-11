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

package org.apache.ignite.internal.ducktest.tests.dns_failure_test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;

/** Handler for {@code java.net.InetAddress$NameService}. */
interface NameServiceHandler extends InvocationHandler {
    /** Intercepts {@code NameService#lookupAllHostAddr}. */
    public InetAddress[] lookupAllHostAddr(String host) throws UnknownHostException;

    /** Intercepts {@code NameService#getHostByAddr}. */
    public String getHostByAddr(byte[] addr) throws UnknownHostException;

    /** Delegate {@code NameService} methods to {@link BlockingNameService}. */
    @Override public default Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();

        if ("lookupAllHostAddr".equals(name))
            return lookupAllHostAddr((String)args[0]);
        else if ("getHostByAddr".equals(name))
            return getHostByAddr((byte[])args[0]);
        else
            throw new UnsupportedOperationException("Unsupported method: " + name);
    }
}
