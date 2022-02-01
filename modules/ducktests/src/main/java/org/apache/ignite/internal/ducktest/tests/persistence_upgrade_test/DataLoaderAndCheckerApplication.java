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

package org.apache.ignite.internal.ducktest.tests.persistence_upgrade_test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Loads/checks the data.
 */
public class DataLoaderAndCheckerApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws IgniteInterruptedCheckedException {
        boolean check = jNode.get("check").asBoolean();

        markInitialized();
        waitForActivation();

        CacheConfiguration<Integer, CustomObject> cacheCfg = new CacheConfiguration<>("cache");

        IgniteCache<Integer, CustomObject> cache = ignite.getOrCreateCache(cacheCfg);

        log.info(check ? "Checking..." : " Preparing...");

        for (int i = 0; i < 10_000; i++) {
            CustomObject obj = new CustomObject(i);

            if (!check)
                cache.put(i, obj);
            else
                assert cache.get(i).equals(obj);
        }

        log.info(check ? "Checked." : " Prepared.");

        while (!terminated())
            U.sleep(100); // Keeping node alive.

        markFinished();
    }

    /**
     *
     */
    private static class CustomObject {
        /** String value. */
        private final String sVal;

        /** Integer value. */
        private final Integer iVal;

        /** Boolean value. */
        private final Boolean bVal;

        /** char value. */
        private final char cVal;

        /** Integer array value. */
        private final Integer[] iArVal;

        /** Integer List. */
        private final List<Integer> iLiVal;

        /**
         * @param idx Index.
         */
        public CustomObject(int idx) {
            sVal = String.valueOf(idx);
            iVal = idx;
            bVal = idx % 2 == 0;
            cVal = sVal.charAt(0);
            iArVal = new Integer[] {idx, idx * 2, idx * idx};
            iLiVal = Arrays.asList(iArVal);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CustomObject obj = (CustomObject)o;
            return cVal == obj.cVal
                && Objects.equals(sVal, obj.sVal)
                && Objects.equals(iVal, obj.iVal)
                && Objects.equals(bVal, obj.bVal)
                && Arrays.equals(iArVal, obj.iArVal)
                && Objects.equals(iLiVal, obj.iLiVal);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Objects.hash(sVal, iVal, bVal, cVal, iLiVal);
            result = 31 * result + Arrays.hashCode(iArVal);
            return result;
        }
    }

}
