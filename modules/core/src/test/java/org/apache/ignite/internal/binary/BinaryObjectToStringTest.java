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

package org.apache.ignite.internal.binary;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import com.google.common.collect.ImmutableMap;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.singletonList;

/** */
public class BinaryObjectToStringTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testToStringInaccessibleOptimizedMarshallerClass() throws Exception {
        IgniteEx ign = startGrid(0);

        ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        assertStringFormContains(new TestContainer(new CustomTestClass()), "CustomTestClass");
        assertStringFormContains(new TestContainer(new ArrayList<>(singletonList(new CustomTestClass()))),
            "ArrayList", "CustomTestClass");
        assertStringFormContains(new TestContainer("abc"), "x=abc");
        assertStringFormContains(new TestContainer(123), "x=123");
        assertStringFormContains(new TestIntContainer(123), "i=123");

        assertStringFormContains(new TestContainer(new int[]{1, 2}), "x=[1, 2]");
        assertStringFormContains(new TestContainer(new Integer[]{1, 2}), "x=[1, 2]");
        assertStringFormContains(new TestContainer(new ArrayList<>(Arrays.asList(1, 2))), "x=ArrayList {1, 2}");
        assertStringFormContains(new TestContainer(new HashSet<>(Arrays.asList(1, 2))), "x=HashSet {1, 2}");
        assertStringFormContains(new TestContainer(new HashMap<>(ImmutableMap.of(1, 2))), "x=HashMap {1=2}");

        ArrayList<Object> nestedList = new ArrayList<>(Arrays.asList(
            new ArrayList<>(Arrays.asList(1, 2)),
            new ArrayList<>(Arrays.asList(3, 4))
        ));
        assertStringFormContains(new TestContainer(nestedList), "x=ArrayList {ArrayList {1, 2}, ArrayList {3, 4}}");

        assertStringFormMatches(new TestContainer(newExtInstance1()), failedStrPattern("ExternalTestClass1"));
        assertStringFormMatches(new TestContainer(newExtInstance2()), failedStrPattern("ExternalTestClass2"));

        assertStringFormMatches(new TestContainer(new Object[] {newExtInstance1()}),
            failedStrPattern("ExternalTestClass1"));
        assertStringFormMatches(new TestContainer(new ArrayList<>(singletonList(newExtInstance1()))),
            failedStrPattern("ExternalTestClass1"));
        assertStringFormMatches(new TestContainer(new HashSet<>(singletonList(newExtInstance1()))),
            failedStrPattern("ExternalTestClass1"));
        assertStringFormMatches(new TestContainer(new HashMap<>(ImmutableMap.of(newExtInstance1(), newExtInstance2()))),
            failedStrPattern("ExternalTestClass1"));

        ArrayList<Object> nestedList2 = new ArrayList<>(singletonList(new ArrayList<>(singletonList(newExtInstance1()))));
        assertStringFormMatches(new TestContainer(nestedList2), failedStrPattern("ExternalTestClass1"));

        assertStringFormMatches(new TestContainer(new TestExternalizable(newExtInstance1())),
            failedStrPattern("ExternalTestClass1"));
    }

    /** */
    private String failedStrPattern(String className) {
        return "org.apache.ignite.internal.binary.BinaryObjectToStringTest\\$TestContainer " +
            "\\[idHash=-?\\d+, hash=-?\\d+, " +
            "x=\\(Failed to create a string representation: class not found " + className + "\\)]";
    }

    /** */
    private static class CustomTestClass {
    }

    /** */
    private static class TestExternalizable implements Externalizable {
        /** */
        private Object x;

        /** */
        private TestExternalizable() {
        }

        /** */
        private TestExternalizable(Object x) {
            this.x = x;
        }

        /** */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(x);
        }

        /** */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            x = in.readObject();
        }
    }

    /** */
    private void assertStringFormContains(Object o, String s0, String... ss) {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        String str = asBinaryObjectString(cache, o);

        assertTrue(str.contains(s0));

        for (String s : ss)
            assertTrue(str.contains(s));
    }

    /** */
    private void assertStringFormMatches(Object o, String pattern) {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertTrue(asBinaryObjectString(cache, o).matches(pattern));
    }

    /** */
    private static String asBinaryObjectString(IgniteCache<Object, Object> cache, Object obj) {
        cache.put(1, obj);

        return cache.withKeepBinary().get(1).toString();
    }

    /** */
    private Object newExtInstance1() throws Exception {
        ClassPool classPool = new ClassPool(ClassPool.getDefault());

        CtClass aClass = classPool.makeClass("ExternalTestClass1");
        aClass.addInterface(classPool.get("java.io.Externalizable"));
        aClass.addField(CtField.make("private int x;", aClass));
        aClass.addConstructor(CtNewConstructor.make("public ExternalTestClass1() {}", aClass));
        aClass.addConstructor(CtNewConstructor.make("public ExternalTestClass1(int x0) { x = x0; }", aClass));
        aClass.addMethod(CtNewMethod.make(
            "public void writeExternal(java.io.ObjectOutput out) throws java.io.IOException { out.writeInt(x); }",
            aClass));
        aClass.addMethod(CtNewMethod.make(
            "public void readExternal(java.io.ObjectInput in) throws java.io.IOException { x = in.readInt(); }",
            aClass));

        ClassLoader extClsLdr = new ClassLoader() {{
            byte[] bytecode = aClass.toBytecode();

            defineClass("ExternalTestClass1", bytecode, 0, bytecode.length);
        }};

        Class<?> extClass = extClsLdr.loadClass("ExternalTestClass1");

        Constructor<?> ctor = extClass.getConstructor(int.class);

        return ctor.newInstance(42);
    }

    /** */
    private Object newExtInstance2() throws Exception {
        ClassPool classPool = new ClassPool(ClassPool.getDefault());

        CtClass aClass = classPool.makeClass("ExternalTestClass2");
        aClass.addInterface(classPool.get("java.io.Serializable"));
        aClass.addField(CtField.make("private int x;", aClass));
        aClass.addConstructor(CtNewConstructor.make("public ExternalTestClass2() {}", aClass));
        aClass.addConstructor(CtNewConstructor.make("public ExternalTestClass2(int x0) { x = x0; }", aClass));
        aClass.addMethod(CtNewMethod.make(
            "private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException { out.writeInt(x); }",
            aClass));
        aClass.addMethod(CtNewMethod.make(
            "private void readObject(java.io.ObjectInputStream in) throws java.io.IOException { x = in.readInt(); }",
            aClass));

        ClassLoader extClsLdr = new ClassLoader() {{
            byte[] bytecode = aClass.toBytecode();

            defineClass("ExternalTestClass2", bytecode, 0, bytecode.length);
        }};

        Class<?> extClass = extClsLdr.loadClass("ExternalTestClass2");

        Constructor<?> ctor = extClass.getConstructor(int.class);

        return ctor.newInstance(42);
    }

    /** */
    private static class TestContainer {
        /** */
        private final Object x;

        /** */
        private TestContainer(Object x) {
            this.x = x;
        }
    }

    /** */
    private static class TestIntContainer {
        /** */
        private final int i;

        /** */
        private TestIntContainer(int i) {
            this.i = i;
        }
    }
}
