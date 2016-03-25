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

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet to test working of Ignite transactions inside web-aplication environment.
 */
@SuppressWarnings("TooBroadScope")
public class TestJtaTxServlet extends HttpServlet {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected void doGet(final HttpServletRequest req, final HttpServletResponse res)
        throws ServletException, IOException {
        final int key = 2;
        final String correctVal = "correct_val";
        final String incorrectVal = "incorrect_val";

        final PrintWriter writer = res.getWriter();

        try {
            Class onePhaseXAResourceClass = Class.forName("com.ibm.tx.jta.OnePhaseXAResource");

            writer.println("Got onePhaseXAResourceClass: " + onePhaseXAResourceClass);
//
//            final Ignite ignite = Ignition.ignite();
//
//            writer.println("Got ignite");
//
//            final IgniteCache<Integer, String> cache = ignite.cache("tx");
//
//            writer.println("Got cache");
//
//            TransactionManager tmMgr = com.ibm.tx.jta.TransactionManagerFactory.getTransactionManager();
//
//            writer.println("Got txMgr");
//
//            tmMgr.begin();
//            cache.put(key, correctVal);
//            writer.println(String.format("Transaction #1. Writing value [%s]", cache.get(key)));
//            writer.println();
//            tmMgr.commit();
//
//            try {
//                tmMgr.begin();
//                writer.println(String.format("Transaction #2. Current value [%s]", cache.get(key)));
//                cache.put(key, incorrectVal);
//                writer.println(String.format("Transaction #2. Writing value [%s]", cache.get(key)));
//                tmMgr.setRollbackOnly();
//                tmMgr.commit();
//            } catch (final RollbackException ignored) {
//                writer.println(String.format("Transaction #2. setRollbackOnly", cache.get(key)));
//            }
//            writer.println();
//
//            tmMgr.begin();
//            writer.println(String.format("Transaction #3. Current value [%s]", cache.get(key)));
//            tmMgr.commit();
        } catch (final Throwable e) {
            e.printStackTrace(writer);
        }
    }
}
