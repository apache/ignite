/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster.extended;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import junit.framework.TestSuite;
import org.apache.ignite.IgniteException;

/**
 *
 */
public abstract class GridActivationCacheAbstractTestSuit {
    /** Tests. */
    private static final List<Class> tests = new ArrayList<>();

    /** Suffix. */
    private static final String SUFFIX = "Ex";

    /**
     * @return Suite.
     */
    protected static TestSuite buildSuite() {
        TestSuite suite = new TestSuite();

        for (Class c : tests)
            suite.addTestSuite(c);

        return suite;
    }

    /**
     * @param c Class.
     */
    protected static void addTest(Class c){
        tests.add(transform(c));
    }


    /**
     * @param c Class to transform.
     * @return Transformed class.
     */
    private static Class transform(Class c){
        try {
            ClassPool pool = ClassPool.getDefault();

            pool.insertClassPath(new ClassClassPath(GridActivationCacheAbstractTestSuit.class));

            String path = c.getProtectionDomain().getCodeSource().getLocation().getPath();

            CtClass ct = pool.get(c.getName());

            String name = c.getName() + SUFFIX;

            CtClass pr = pool.get(GridActivateExtensionTest.class.getName());

            pr.setName(name);

            pr.setSuperclass(ct);

            pr.writeFile(path);

            return Class.forName(name);
        }
        catch (IOException e) {
            System.out.println("Io exception: " + e.getMessage());

            throw new IgniteException(e);
        }
        catch (CannotCompileException e) {
            System.out.println("Cannot compile exception: " + e.getMessage());

            throw new IgniteException(e);
        }
        catch (NotFoundException e) {
            System.out.println("Not found exception: " + e.getMessage());

            throw new IgniteException(e);
        }
        catch (ClassNotFoundException e) {
            System.out.println("Class not found exception: " + e.getMessage());

            throw new IgniteException(e);
        }
    }
}
