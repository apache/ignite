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

package org.apache.ignite.logger.log4j;

import java.io.File;
import java.net.MalformedURLException;
import org.apache.ignite.IgniteCheckedException;
import org.junit.Test;

/**
 * Checking Log4JLogger constructors accepting watchDelay parameter.
 */
public class GridLog4jWatchDelayTest {
    /** Path to log4j configuration file. */
    private static final String LOG_CONFIG = "modules/log4j/src/test/config/log4j-info.xml";

    /** Check negative watchDelay in String constructor. */
    @Test(expected = IgniteCheckedException.class)
    public void testNegativeWatchDelayString() throws IgniteCheckedException {
        new Log4JLogger(LOG_CONFIG, -1);
    }

    /** Check negative watchDelay in String constructor. */
    @Test(expected = IgniteCheckedException.class)
    public void testNegativeWatchDelayFile() throws IgniteCheckedException {
        new Log4JLogger(new File(LOG_CONFIG), -1);
    }

    /** Check negative watchDelay in String constructor. */
    @Test(expected = IgniteCheckedException.class)
    public void testNegativeWatchDelayUrl() throws IgniteCheckedException, MalformedURLException {
        new Log4JLogger(new File(LOG_CONFIG).toURI().toURL(), -1);
    }
}
