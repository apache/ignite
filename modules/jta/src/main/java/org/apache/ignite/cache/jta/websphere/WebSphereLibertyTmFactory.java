/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
