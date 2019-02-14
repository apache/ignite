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
package org.apache.ignite.testframework.junits;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

/**
 * Runner for use with test suite classes that implement a static {@code suite()} method for providing list of classes
 * to execute in a way resembling how it was done in {@code org.junit.runners.AllTests}. For example:
 * <pre>
 * &#064;RunWith(DynamicSuite.class)
 * public class SomeTestSuite {
 *    public static List&lt;Class&lt;?&gt;&gt; suite() {
 *       ...
 *    }
 * }
 * </pre>
 */
public class DynamicSuite extends Suite {
    /** */
    private static final String SUITE_METHOD_NAME = "suite";

    /** */
    public DynamicSuite(Class<?> cls) throws InitializationError {
        super(cls, testClasses(cls));
    }

    /** */
    private static Class[] testClasses(Class<?> cls) {
        List<Class<?>> testClasses;

        try {
            Method mtd = cls.getMethod(SUITE_METHOD_NAME);

            testClasses = (List<Class<?>>)mtd.invoke(null);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("No public static 'suite' method was found in provided suite class: "
                + cls.getSimpleName());
        }

        assert testClasses != null : "Null list of test classes was obtained from suite class: " + cls.getSimpleName();

        return testClasses.toArray(new Class[] {null});
    }
}
