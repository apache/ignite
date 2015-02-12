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

package org.apache.ignite.internal;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * Tests version methods.
 */
public class GridVersionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testVersions() throws Exception {
        String propVal = System.getProperty(IGNITE_UPDATE_NOTIFIER);

        System.setProperty(IGNITE_UPDATE_NOTIFIER, "true");

        try {
            IgniteEx ignite = (IgniteEx)startGrid();

            IgniteProductVersion currVer = ignite.version();

            String newVer = null;

            for (int i = 0; i < 30; i++) {
                newVer = ignite.latestVersion();

                if (newVer != null)
                    break;

                U.sleep(100);
            }

            info("Versions [cur=" + currVer + ", latest=" + newVer + ']');

            assertNotNull(newVer);
            assertNotSame(currVer.toString(), newVer);
        }
        finally {
            stopGrid();

            if (propVal != null)
                System.setProperty(IGNITE_UPDATE_NOTIFIER, propVal);
            else
                System.clearProperty(IGNITE_UPDATE_NOTIFIER);
        }
    }
}
