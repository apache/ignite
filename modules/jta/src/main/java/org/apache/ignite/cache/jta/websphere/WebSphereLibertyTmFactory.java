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

package org.apache.ignite.cache.jta.websphere;

import java.lang.reflect.InvocationTargetException;
import javax.cache.configuration.Factory;
import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteException;

/**
 * Implementation of Transaction Manager factory that should used within WebSphere Liberty.
 * <h2 class="header">Java Configuration</h2>
 * <pre name="code" class="java">
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * TransactionConfiguration txCfg = new TransactionConfiguration();
 *
 * txCfg.setTxManagerFactory(new WebSphereLibertyTmFactory());
 *
 * cfg.setTransactionConfiguration(new txCfg);
 * </pre>
 * <h2 class="header">Spring Configuration</h2>
 * <pre name="code" class="xml">
 * &lt;bean id="ignite.cfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *         ...
 *         &lt;property name="transactionConfiguration"&gt;
 *             &lt;bean class="org.apache.ignite.cache.jta.websphere.WebSphereLibertyTmFactory"/&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>*
 */
public class WebSphereLibertyTmFactory implements Factory<TransactionManager> {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    private static final String CLS = "com.ibm.tx.jta.TransactionManagerFactory";

    /** */
    private static final String MTD = "getTransactionManager";

    /** {@inheritDoc} */
    @Override public TransactionManager create() {
        try {
            return (TransactionManager)Class.forName(CLS).getMethod(MTD).invoke(null);
        }
        catch (ClassNotFoundException e) {
            throw new IgniteException("Failed to find class: " + CLS, e);
        }
        catch (NoSuchMethodException e) {
            throw new IgniteException("Failed to find method: " + MTD, e);
        }
        catch (InvocationTargetException | IllegalAccessException e) {
            throw new IgniteException("Failed to invoke method: " + MTD, e);
        }
    }
}
