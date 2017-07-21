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

package org.apache.ignite.cache.jta.jndi;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Implementation of {@code Factory<TransactionManager>} interface that is using JNDI names to find TM.
 * <p>
 * Note that {@link #create()} method iterates by JNDI names and returns the first found
 * {@link TransactionManager} instance at context.
 */
public class CacheJndiTmFactory implements Factory<TransactionManager> {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    private String[] jndiNames;

    /** */
    private Map<?, ?> environment;

    /**
     * Creates uninitialized jndi TM lookup.
     */
    public CacheJndiTmFactory() {
        /* No-op. */
    }

    /**
     * Creates generic TM lookup with given jndi names.
     *
     * @param jndiNames JNDI names that is used to find TM.
     */
    public CacheJndiTmFactory(String... jndiNames) {
        this.jndiNames = jndiNames;
    }

    /**
     * Gets a list of JNDI names.
     *
     * @return List of JNDI names that is used to find TM.
     */
    public String[] getJndiNames() {
        return jndiNames;
    }

    /**
     * Sets JNDI names used by this TM factory.
     *
     * @param jndiNames JNDI names that is used to find TM.
     */
    public void setJndiNames(String... jndiNames) {
        this.jndiNames = jndiNames;
    }

    /**
     * Gets initial context environment map.
     *
     * @return Initial context environment map.
     */
    public Map<?, ?> getInitialContextEnvironment() {
        return environment;
    }

    /**
     * Sets initial context environment map that will be used
     * in {@link InitialContext#InitialContext(Hashtable)} constructor.
     *
     * @param environment Initial context environment map.
     */
    public void setInitialContextEnvironment(Map<?, ?> environment) {
        this.environment = environment;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("UseOfObsoleteCollectionType")
    @Override public TransactionManager create() {
        assert jndiNames != null;
        assert jndiNames.length != 0;

        InitialContext ctx;

        try {
            ctx = new InitialContext(environment == null ? null : new Hashtable<>(environment));
        }
        catch (NamingException e) {
            throw new IgniteException("Failed to instantiate InitialContext: " + environment, e);
        }

        for (String s : jndiNames) {
            Object obj;

            try {
                obj = ctx.lookup(s);
            }
            catch (NamingException e) {
                U.warn(null, "Failed to lookup resourse: " + e);

                continue;
            }

            if (obj != null && obj instanceof TransactionManager)
                return (TransactionManager) obj;
        }

        throw new IgniteException("Failed to lookup TM by: " + Arrays.toString(jndiNames));
    }
}
