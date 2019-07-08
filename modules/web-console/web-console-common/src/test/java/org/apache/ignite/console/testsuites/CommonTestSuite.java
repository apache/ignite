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

package org.apache.ignite.console.testsuites;

import org.apache.ignite.console.messages.WebConsoleMessageSourceTest;
import org.apache.ignite.console.utils.UtilsTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Web Console "commons" test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    UtilsTest.class,
    WebConsoleMessageSourceTest.class
})
public class CommonTestSuite {
}
