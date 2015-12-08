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

package org.apache.ignite.internal.processors.cache;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.objectweb.jotm.Jotm;

/**
 *
 */
public class CacheJndiTmFactorySelfTest
//    extends GridCacheAbstractSelfTest
{
    public static final String CTX_NAME = "java:/comp/env/jdbc/mytm";
    /** Java Open Transaction Manager facade. */
    protected static Jotm jotm;
//
//    /** {@inheritDoc} */
//    @Override protected int gridCount() {
//        return 2;
//    }

    public static void main(String[] args) throws NamingException {
        // rcarver - setup the jndi context and the datasource
        // rcarver - setup the jndi context and the datasource
        try {
            // Create initial context
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                "org.apache.naming.java.javaURLContextFactory");
            System.setProperty(Context.URL_PKG_PREFIXES,
                "org.apache.naming");
            InitialContext ic = new InitialContext();

            ic.createSubcontext("java:");
            ic.createSubcontext("java:/comp");
            ic.createSubcontext("java:/comp/env");
            ic.createSubcontext("java:/comp/env/tm");

            // Construct TM
            ic.bind("java:/comp/env/tm/jotm", "Hello Artem!");
        } catch (NamingException ex) {
            ex.printStackTrace();
//            Logger.getLogger(MyDAOTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        Context initContext = new InitialContext();
        Context webContext = (Context)initContext.lookup("java:/comp/env");

        Object o = webContext.lookup("tm/jotm");

        System.out.println(">>>>>" + o);
    }

    /**
     * @throws Exception If failed.
     */
    public void testName() throws Exception {
        // rcarver - setup the jndi context and the datasource
        try {
            // Create initial context
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                "org.apache.naming.java.javaURLContextFactory");
            System.setProperty(Context.URL_PKG_PREFIXES,
                "org.apache.naming");
            InitialContext ic = new InitialContext();

            ic.createSubcontext("java:");
            ic.createSubcontext("java:/comp");
            ic.createSubcontext("java:/comp/env");
            ic.createSubcontext("java:/comp/env/jdbc");

            // Construct TM
            jotm = new Jotm(true, false);

            ic.bind("java:/comp/env/jdbc/nameofmyjdbcresource", jotm);
        } catch (NamingException ex) {
            ex.printStackTrace();
//            Logger.getLogger(MyDAOTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        Context initContext = new InitialContext();
        Context webContext = (Context)initContext.lookup("java:/comp/env");

        Jotm jotm = (Jotm)webContext.lookup("jdbc/nameofmyjdbcresource");

        System.out.println(">>>>>" + jotm);
    }
}
