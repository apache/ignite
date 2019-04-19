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

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3.client;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class to simulate the functionality of {@link ObjectListing}.
 */
public class DummyObjectListing extends ObjectListing {
    /** Iterator over the S3 object summaries. */
    private Iterator<S3ObjectSummary> objSummariesIter;

    /**
     * Constructor
     *
     * @param objSummaries Iterator over the S3 object summaries.
     */
    private DummyObjectListing(Iterator<S3ObjectSummary> objSummaries) {
        this.objSummariesIter = objSummaries;
    }

    /**
     * Creates an instance of {@link DummyObjectListing}. The object summaries are created using the given  bucket name
     * and object keys.
     *
     * @param bucketName AWS Bucket name.
     * @param keys The keys in the bucket.
     * @return Instance of this object.
     */
    static DummyObjectListing of(String bucketName, Set<String> keys) {
        List<S3ObjectSummary> objSummaries = keys.stream().map(key -> {
            S3ObjectSummary s3ObjSummary = new S3ObjectSummary();
            s3ObjSummary.setBucketName(bucketName);
            s3ObjSummary.setKey(key);
            return s3ObjSummary;
        }).collect(Collectors.toList());

        return new DummyObjectListing(objSummaries.iterator());
    }

    /** {@inheritDoc} */
    @Override public List<S3ObjectSummary> getObjectSummaries() {
        if (objSummariesIter.hasNext()) {
            S3ObjectSummary s3ObjSummary = objSummariesIter.next();

            List<S3ObjectSummary> list = new LinkedList<>();

            list.add(s3ObjSummary);

            return list;
        }
        else
            return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public boolean isTruncated() {
        return objSummariesIter.hasNext();
    }
}
