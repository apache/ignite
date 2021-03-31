/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.util.tostring;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.TxEntryValueHolder;
import org.apache.ignite.internal.processors.cacheobject.UserCacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cacheobject.UserCacheObjectImpl;
import org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Objects;
import java.util.function.BiConsumer;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.SensitiveDataLogging.HASH;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.SensitiveDataLogging.NONE;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.SensitiveDataLogging.PLAIN;

/**
 * Tests for output of {@code toString()} depending on the value of {@link IGNITE_SENSITIVE_DATA_LOGGING}
 */
public class SensitiveDataToStringTest extends GridCommonAbstractTest {
    /** Random int. */
    int rndInt0 = 54321;

    /** Random int. */
    int rndInt1 = 112233;

    /** Random int. */
    int rndInt2 = 334455;

    /** Random byte array. */
    byte[] rndArray = new byte[] { (byte) 22, (byte) 111};

    /** Random string. */
    String rndString = "qwer";

    /** */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testSensitivePropertiesResolving0() {
        assertTrue(S.getSensitiveDataLogging().toString(), S.getSensitiveDataLogging() == HASH);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testSensitivePropertiesResolving1() {
        assertTrue(S.getSensitiveDataLogging().toString(), S.getSensitiveDataLogging() == PLAIN);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testSensitivePropertiesResolving2() {
        assertTrue(S.getSensitiveDataLogging().toString(), S.getSensitiveDataLogging() == HASH);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testSensitivePropertiesResolving3() {
        assertTrue(S.getSensitiveDataLogging().toString(), S.getSensitiveDataLogging() == NONE);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "true")
    public void testSensitivePropertiesResolving4() {
        assertTrue(S.getSensitiveDataLogging().toString(), S.getSensitiveDataLogging() == PLAIN);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
    public void testSensitivePropertiesResolving5() {
        assertTrue(S.getSensitiveDataLogging().toString(), S.getSensitiveDataLogging() == NONE);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testSensitivePropertiesResolving6() {
        assertTrue(S.getSensitiveDataLogging().toString(), S.getSensitiveDataLogging() == NONE);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "true")
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testSensitivePropertiesResolving7() {
        assertTrue(S.getSensitiveDataLogging().toString(), S.getSensitiveDataLogging() == PLAIN);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testCacheObjectImplWithSensitive() {
        testCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.contains(object.toString())));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testCacheObjectImplWithHashSensitive() {
        testCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(IgniteUtils.hash(object)))));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testCacheObjectImplWithoutSensitive() {
        testCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("CacheObject")));
    }

    /** */
    private void testCacheObjectImpl(BiConsumer<String, Object> checker) {
        Person person = new Person(rndInt0, rndString);

        CacheObjectImpl testObject = new CacheObjectImpl(person, null);
        checker.accept(testObject.toString(), person);

        testObject = new UserCacheObjectImpl(person, null);
        checker.accept(testObject.toString(), person);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testKeyCacheObjectImplWithSensitive() {
        testKeyCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.contains(object.toString())));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testKeyCacheObjectImplWithHashSensitive() {
        testKeyCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(IgniteUtils.hash(object)))));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testKeyCacheObjectImplWithoutSensitive() {
        testKeyCacheObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("KeyCacheObject")));
    }

    /** */
    private void testKeyCacheObjectImpl(BiConsumer<String, Object> checker) {
        Person person = new Person(rndInt0, rndString);

        KeyCacheObjectImpl testObject = new KeyCacheObjectImpl(person, null, rndInt1);
        checker.accept(testObject.toString(), person);

        testObject = new UserKeyCacheObjectImpl(person, rndInt1);
        checker.accept(testObject.toString(), person);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testBinaryEnumObjectImplWithSensitive() {
        testBinaryEnumObjectImpl((strToCheck, object) -> {
            assertTrue(strToCheck, strToCheck.contains("clsName=null"));
            assertTrue(strToCheck, strToCheck.contains("ordinal=0"));
        });
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testBinaryEnumObjectImplWithHashSensitive() {
        testBinaryEnumObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(IgniteUtils.hash(object)))));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testBinaryEnumObjectImplWithoutSensitive() {
        testBinaryEnumObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("BinaryEnum")));
    }

    /** */
    private void testBinaryEnumObjectImpl(BiConsumer<String, Object> checker) {
        BinaryEnumObjectImpl testObject = new BinaryEnumObjectImpl();
        checker.accept(testObject.toString(), testObject);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testBinaryObjectImplWithSensitive() throws Exception {
        testBinaryObjectImpl((strToCheck, object) -> {
            assertTrue(strToCheck, strToCheck.contains("orgId=" + rndInt0));
            assertTrue(strToCheck, strToCheck.contains("name=" + rndString));
        });
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testBinaryObjectImplWithHashSensitive() throws Exception {
        testBinaryObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(IgniteUtils.hash(object)))));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testBinaryObjectImplWithoutSensitive() throws Exception {
        testBinaryObjectImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("BinaryObject")));
    }

    /** */
    private void testBinaryObjectImpl(BiConsumer<String, Object> checker) throws Exception {
        IgniteEx grid = startGrid(0);
        IgniteBinary binary = grid.binary();
        BinaryObject binPerson = binary.toBinary(new Person(rndInt0, rndString));
        assertTrue(binPerson.getClass().getSimpleName(), binPerson instanceof BinaryObjectImpl);
        checker.accept(binPerson.toString(), binPerson);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testBinaryObjectOffheapImplWithoutSensitive() {
        testBinaryObjectOffheapImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("BinaryObject")));
    }

    /** */
    private void testBinaryObjectOffheapImpl(BiConsumer<String, Object> checker) {
        BinaryObjectOffheapImpl testObject = new BinaryObjectOffheapImpl(null, rndInt0, rndInt1, rndInt2);
        checker.accept(testObject.toString(), testObject);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testCacheObjectByteArrayImplWithSensitive() {
        testCacheObjectByteArrayImpl((strToCheck, object) -> {
            assertTrue(strToCheck, strToCheck.contains("arrLen=" + 2));
        });
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testCacheObjectByteArrayImplWithHashSensitive() {
        testCacheObjectByteArrayImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals(String.valueOf(IgniteUtils.hash(rndArray)))));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testCacheObjectByteArrayImplWithoutSensitive() {
        testCacheObjectByteArrayImpl((strToCheck, object) -> assertTrue(strToCheck, strToCheck.equals("CacheObject")));
    }

    /** */
    private void testCacheObjectByteArrayImpl(BiConsumer<String, Object> checker) {
        CacheObjectByteArrayImpl testObject = new CacheObjectByteArrayImpl(rndArray);
        checker.accept(testObject.toString(), testObject);

        testObject = new UserCacheObjectByteArrayImpl(rndArray);
        checker.accept(testObject.toString(), testObject);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testIgniteTxKeyWithSensitive() {
        testIgniteTxKey((strToCheck, object) -> {
            assertTrue(strToCheck,
                    strToCheck.contains("key") && strToCheck.contains("" + rndInt0) && strToCheck.contains(rndString));
        });
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testIgniteTxKeyWithHashSensitive() {
        testIgniteTxKey((strToCheck, object) -> assertTrue(strToCheck, strToCheck.contains("key=" + IgniteUtils.hash(object))));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testIgniteTxKeyWithoutSensitive() {
        testIgniteTxKey((strToCheck, object) -> assertTrue(strToCheck, !strToCheck.contains("key") &&
                !strToCheck.contains("" + rndInt0) &&
                !strToCheck.contains(rndString) &&
                !strToCheck.contains("" + IgniteUtils.hash(object))
        ));
    }

    /** */
    private void testIgniteTxKey(BiConsumer<String, Object> checker) {
        KeyCacheObjectImpl keyCacheObject = new KeyCacheObjectImpl(new Person(rndInt0, rndString), null, rndInt1);
        IgniteTxKey txKey = new IgniteTxKey(keyCacheObject, rndInt2);
        checker.accept(txKey.toString(), keyCacheObject);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testTxEntryValueHolderWithSensitive() {
        testTxEntryValueHolder((strToCheck, object) -> {
            assertTrue(strToCheck,
                    strToCheck.contains("val") && strToCheck.contains("" + rndInt0) && strToCheck.contains(rndString));
        });
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testTxEntryValueHolderWithHashSensitive() {
        testTxEntryValueHolder((strToCheck, object) -> assertTrue(strToCheck, strToCheck.contains("val=" + IgniteUtils.hash(object))));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testTxEntryValueHolderWithoutSensitive() {
        testTxEntryValueHolder((strToCheck, object) -> assertTrue(strToCheck, !strToCheck.contains("val") &&
                !strToCheck.contains("" + rndInt0) &&
                !strToCheck.contains(rndString) &&
                !strToCheck.contains("" + IgniteUtils.hash(object))
        ));
    }

    /** */
    private void testTxEntryValueHolder(BiConsumer<String, Object> checker) {
        final TxEntryValueHolder txEntryValue = new TxEntryValueHolder();
        final Person person = new Person(rndInt0, rndString);
        final CacheObjectImpl cacheObject = new CacheObjectImpl(person, null);
        txEntryValue.value(cacheObject);
        checker.accept(txEntryValue.toString(), person);
    }

    /** */
    static class Person {
        /** Id organization. */
        int orgId;

        /** Person name. */
        String name;

        /**
         * Constructor.
         *
         * @param orgId Id organization.
         * @param name Person name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(orgId, name);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person{" +
                    "orgId=" + orgId +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
