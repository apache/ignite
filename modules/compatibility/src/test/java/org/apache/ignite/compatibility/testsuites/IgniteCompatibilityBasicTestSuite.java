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

package org.apache.ignite.compatibility.testsuites;

import org.apache.ignite.compatibility.clients.JavaThinCompatibilityTest;
import org.apache.ignite.compatibility.clients.JdbcThinCompatibilityTest;
import org.apache.ignite.compatibility.persistence.CompoundIndexCompatibilityTest;
import org.apache.ignite.compatibility.persistence.FoldersReuseCompatibilityTest;
import org.apache.ignite.compatibility.persistence.IgnitePKIndexesMigrationToUnwrapPkTest;
import org.apache.ignite.compatibility.persistence.IndexTypesCompatibilityTest;
import org.apache.ignite.compatibility.persistence.InlineJavaObjectCompatibilityTest;
import org.apache.ignite.compatibility.persistence.MetaStorageCompatibilityTest;
import org.apache.ignite.compatibility.persistence.MigratingToWalV2SerializerWithCompactionTest;
import org.apache.ignite.compatibility.persistence.MoveBinaryMetadataCompatibility;
import org.apache.ignite.compatibility.persistence.PersistenceBasicCompatibilityTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Compatibility tests basic test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    PersistenceBasicCompatibilityTest.class,
    InlineJavaObjectCompatibilityTest.class,
    IndexTypesCompatibilityTest.class,
    FoldersReuseCompatibilityTest.class,
    MigratingToWalV2SerializerWithCompactionTest.class,
    MetaStorageCompatibilityTest.class,
    MoveBinaryMetadataCompatibility.class,
    JdbcThinCompatibilityTest.class,
    JavaThinCompatibilityTest.class,
    IgnitePKIndexesMigrationToUnwrapPkTest.class,
    CompoundIndexCompatibilityTest.class,
})
public class IgniteCompatibilityBasicTestSuite {
}
