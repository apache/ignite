/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3.client;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Class to test {@link DummyObjectListing}.
 */
@RunWith(JUnit4.class)
public class DummyObjectListingTest extends GridCommonAbstractTest {
    /**
     * Test cases for various object listing functions for S3 bucket.
     */
    @Test
    public void testDummyObjectListing() {
        Set<String> fakeKeyPrefixSet = new HashSet<>();

        fakeKeyPrefixSet.add("/test/path/val");
        fakeKeyPrefixSet.add("/test/val/test/path");
        fakeKeyPrefixSet.add("/test/test/path/val");

        ObjectListing listing = DummyObjectListing.of("bucket", fakeKeyPrefixSet);

        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        assertFalse("'testBucket' contains keys", summaries.isEmpty());
        assertTrue("'testBucket' contains more keys to fetch", listing.isTruncated());
        assertTrue(fakeKeyPrefixSet.contains(summaries.get(0).getKey()));

        summaries = listing.getObjectSummaries();

        assertFalse("'testBucket' contains keys", summaries.isEmpty());
        assertTrue("'testBucket' contains more keys to fetch", listing.isTruncated());
        assertTrue(fakeKeyPrefixSet.contains(summaries.get(0).getKey()));

        summaries = listing.getObjectSummaries();

        assertFalse("'testBucket' contains keys", summaries.isEmpty());
        assertFalse("'testBucket' does not contain anymore keys", listing.isTruncated());
        assertTrue(fakeKeyPrefixSet.contains(summaries.get(0).getKey()));

        listing = DummyObjectListing.of("bucket", new HashSet<>());

        summaries = listing.getObjectSummaries();

        assertTrue("'testBucket' does not contains keys", summaries.isEmpty());
        assertFalse("'testBucket' does not contain anymore keys", listing.isTruncated());
    }
}
