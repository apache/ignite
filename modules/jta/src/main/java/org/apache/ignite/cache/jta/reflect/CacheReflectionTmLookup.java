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

package org.apache.ignite.cache.jta.reflect;

import java.lang.reflect.InvocationTargetException;
import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.jta.CacheTmLookup;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Implementation of {@link org.apache.ignite.cache.jta.CacheTmLookup} interface that attempts to obtain
 * JTA manager by calling static method on the class.
 */
public class CacheReflectionTmLookup implements CacheTmLookup {
    /** */
    private String cls;

    /** */
    private String mtd;

    /**
     * Creates uninitialized reflection TM lookup.
     */
    public CacheReflectionTmLookup() { /* No-op. */ }

    /**
     * Creates generic TM lookup with given class and method name.
     *
     * @param cls Class name.
     * @param mtd Method name on that the class.
     */
    public CacheReflectionTmLookup(String cls, String mtd) {
        A.notNull(cls, "cls");
        A.notNull(mtd, "mtd");

        this.cls = cls;
        this.mtd = mtd;
    }

    /**
     * Gets class name to use.
     *
     * @return Class name to use.
     */
    public String getClassName() {
        return cls;
    }

    /**
     * Sets class name to use.
     *
     * @param cls Class name to use.
     */
    public void setClassName(String cls) {
        A.notNull(cls, "cls");

        this.cls = cls;
    }

    /**
     * Gets method name.
     *
     * @return Method name to use.
     */
    public String getMethodName() {
        return mtd;
    }

    /**
     * Sets method name.
     *
     * @param mtd Method name to use.
     */
    public void setMethodName(String mtd) {
        A.notNull(mtd, "mtd");

        this.mtd = mtd;
    }

    /** {@inheritDoc} */
    @Override public TransactionManager getTm() throws IgniteException {
        assert cls != null;
        assert mtd != null;

        try {
            return (TransactionManager)Class.forName(cls).getMethod(mtd).invoke(null);
        }
        catch (ClassNotFoundException e) {
            throw new IgniteException("Failed to find class: " + cls, e);
        }
        catch (NoSuchMethodException e) {
            throw new IgniteException("Failed to find method: " + mtd, e);
        }
        catch (InvocationTargetException | IllegalAccessException e) {
            throw new IgniteException("Failed to invoke method: " + mtd, e);
        }
    }
}