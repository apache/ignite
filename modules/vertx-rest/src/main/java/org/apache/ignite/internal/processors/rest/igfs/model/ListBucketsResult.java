package org.apache.ignite.internal.processors.rest.igfs.model;

import java.util.List;

public class ListBucketsResult {
    private List<Bucket> buckets;

    public ListBucketsResult(){

    }

    public ListBucketsResult(List<Bucket> buckets) {
        this.buckets = buckets;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<Bucket> buckets) {
        this.buckets = buckets;
    }
}
