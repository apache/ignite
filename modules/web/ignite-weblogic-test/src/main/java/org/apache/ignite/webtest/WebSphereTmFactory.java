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

package org.apache.ignite.webtest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import javax.cache.configuration.Factory;
import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteException;

/**
 *
 */
public class WebSphereTmFactory implements Factory<TransactionManager> {
    /** */
    private static final long serialVersionUID = 0;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public TransactionManager create() {
        System.out.println(">>>>> DEBUG_INFO 1");

        try {
            Class clazz = Class.forName("com.ibm.tx.jta.impl.TranManagerSet");

            Method m = clazz.getMethod("instance", (Class[])null);

            TransactionManager tranMgr = (TransactionManager)m.invoke((Object)null, (Object[])null);

            return new WebSphereTransactionManager(tranMgr);
        }
        catch (SecurityException | ClassNotFoundException | IllegalArgumentException | NoSuchMethodException
            | InvocationTargetException | IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }
}
