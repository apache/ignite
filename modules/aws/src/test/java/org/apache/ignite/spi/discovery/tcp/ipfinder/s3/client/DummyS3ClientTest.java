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

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3.client;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Class to test {@link DummyS3Client}.
 */
public class DummyS3ClientTest extends GridCommonAbstractTest {
    /** Instance of {@link DummyS3Client} to be used for tests. */
    private DummyS3Client s3;

    /** Holds fake key prefixes. */
    private Set<String> fakeKeyPrefixSet;

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        fakeKeyPrefixSet = new HashSet<>();
        fakeKeyPrefixSet.add("/test/path/val");
        fakeKeyPrefixSet.add("/test/val/test/path");
        fakeKeyPrefixSet.add("/test/test/path/val");

        Map<String, Set<String>> fakeObjMap = new HashMap<>();

        fakeObjMap.put("testBucket", fakeKeyPrefixSet);

        s3 = new DummyS3Client(fakeObjMap);
    }

    /**
     * Test cases to check the 'doesBucketExist' method.
     */
    public void testDoesBucketExist() {
        assertTrue("The bucket 'testBucket' should exist", s3.doesBucketExist("testBucket"));
        assertFalse("The bucket 'nonExistentBucket' should not exist", s3.doesBucketExist("nonExistentBucket"));
    }

    /**
     * Test cases for various object listing functions for S3 bucket.
     */
    public void testListObjects() {
        ObjectListing listing = s3.listObjects("testBucket");

        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        assertFalse("'testBucket' contains keys", summaries.isEmpty());
        assertTrue("'testBucket' contains more keys to fetch", listing.isTruncated());
        assertTrue(fakeKeyPrefixSet.contains(summaries.get(0).getKey()));

        listing = s3.listNextBatchOfObjects(listing);

        summaries = listing.getObjectSummaries();

        assertFalse("'testBucket' contains keys", summaries.isEmpty());
        assertTrue("'testBucket' contains more keys to fetch", listing.isTruncated());
        assertTrue(fakeKeyPrefixSet.contains(summaries.get(0).getKey()));

        listing = s3.listNextBatchOfObjects(listing);

        summaries = listing.getObjectSummaries();

        assertFalse("'testBucket' contains keys", summaries.isEmpty());
        assertFalse("'testBucket' does not contain anymore keys", listing.isTruncated());
        assertTrue(fakeKeyPrefixSet.contains(summaries.get(0).getKey()));

        try {
            s3.listObjects("nonExistentBucket");
        }
        catch (AmazonS3Exception e) {
            assertTrue(e.getMessage().contains("The specified bucket does not exist"));
        }
    }

    /**
     * Test cases for various object listing functions for S3 bucket and key prefix.
     */
    public void testListObjectsWithAPrefix() {
        ObjectListing listing = s3.listObjects("testBucket", "/test");

        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        assertFalse("'testBucket' must contain key with prefix '/test'", summaries.isEmpty());
        assertTrue("'testBucket' contains more keys with prefix '/test'", listing.isTruncated());
        assertTrue(fakeKeyPrefixSet.contains(summaries.get(0).getKey()));

        listing = s3.listNextBatchOfObjects(listing);

        summaries = listing.getObjectSummaries();

        assertFalse("'testBucket' must contain key with prefix '/test'", summaries.isEmpty());
        assertTrue("'testBucket' contains more keys with prefix '/test'", listing.isTruncated());
        assertTrue(fakeKeyPrefixSet.contains(summaries.get(0).getKey()));

        listing = s3.listNextBatchOfObjects(listing);

        summaries = listing.getObjectSummaries();

        assertFalse("'testBucket' must contain key with prefix '/test'", summaries.isEmpty());
        assertFalse("'testBucket' does not contain anymore keys with prefix '/test'", listing.isTruncated());
        assertTrue(fakeKeyPrefixSet.contains(summaries.get(0).getKey()));

        listing = s3.listObjects("testBucket", "/test/path");

        summaries = listing.getObjectSummaries();

        assertFalse("'testBucket' must contain key with prefix '/test'", summaries.isEmpty());
        assertFalse("'testBucket' does not contain anymore keys with prefix '/test/path'", listing.isTruncated());
        assertEquals("/test/path/val", summaries.get(0).getKey());

        listing = s3.listObjects("testBucket", "/non/existent/test/path");

        summaries = listing.getObjectSummaries();

        assertTrue("'testBucket' must not contain key with prefix '/non/existent/test/path'", summaries.isEmpty());
        assertFalse("'testBucket' does not contain anymore keys with prefix '/non/existent/test/path'", listing.isTruncated());

        try {
            s3.listObjects("nonExistentBucket", "/test");
        }
        catch (AmazonS3Exception e) {
            assertTrue(e.getMessage().contains("The specified bucket does not exist"));
        }
    }

    /**
     * Test case to check if a bucket is created properly.
     */
    public void testCreateBucket() {
        s3.createBucket("testBucket1");

        assertTrue("The bucket 'testBucket1' should exist", s3.doesBucketExist("testBucket1"));

        try {
            s3.createBucket("testBucket");
        }
        catch (AmazonS3Exception e) {
            assertTrue(e.getMessage().contains("The specified bucket already exist"));
        }
    }
}
