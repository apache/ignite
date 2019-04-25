/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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