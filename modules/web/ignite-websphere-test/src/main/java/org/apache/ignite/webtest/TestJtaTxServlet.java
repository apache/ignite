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
import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;
import com.ibm.tx.jta.TransactionManagerFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

/**
 * Servlet to test working of Ignite transactions inside web-application environment.
 */
@SuppressWarnings("TooBroadScope")
public class TestJtaTxServlet extends HttpServlet {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected void doGet(final HttpServletRequest req, final HttpServletResponse res)
        throws ServletException, IOException {
        final int key1 = 1;
        final int key2 = 2;

        final String correctVal1 = "correct_val1";
        final String correctVal2 = "correct_val1";
        final String incorrectVal1 = "incorrect_val2";
        final String incorrectVal2 = "incorrect_val2";

        final PrintWriter writer = res.getWriter();

        try {
            final Ignite ignite = Ignition.ignite();

            final IgniteCache<Integer, String> cache = ignite.cache("tx");

            TransactionManager tmMgr = TransactionManagerFactory.getTransactionManager();

            tmMgr.begin();

            cache.put(key1, correctVal1);
            cache.put(key2, correctVal2);

            writer.println("Transaction #1. Put values [key1=" + key1 + ", val1=" + cache.get(key1)
                + ", key2=" + key2 + ", val2=" + cache.get(key2) + "]");
            writer.println();

            tmMgr.commit();

            try {
                tmMgr.begin();

                writer.println("Transaction #2. Current values [key1=" + key1 + ", val1=" + cache.get(key1)
                    + ", key2=" + key2 + ", val2=" + cache.get(key2) + "]");

                cache.put(key1, incorrectVal1);
                cache.put(key2, incorrectVal2);

                writer.println("Transaction #2. Put values [key1=" + key1 + ", val1=" + cache.get(key1)
                    + ", key2=" + key2 + ", val2=" + cache.get(key2) + "]");

                tmMgr.setRollbackOnly();

                tmMgr.commit();
            }
            catch (final RollbackException ignored) {
                writer.println("Transaction #2. setRollbackOnly [key1=" + key1 + ", val1=" + cache.get(key1)
                    + ", key2=" + key2 + ", val2=" + cache.get(key2) + "]");
            }

            writer.println();

            tmMgr.begin();

            writer.println("Transaction #2. Current values [key1=" + key1 + ", val1=" + cache.get(key1)
                + ", key2=" + key2 + ", val2=" + cache.get(key2) + "]");

            tmMgr.commit();
        }
        catch (final Throwable e) {
            e.printStackTrace(writer);
        }
    }
}
