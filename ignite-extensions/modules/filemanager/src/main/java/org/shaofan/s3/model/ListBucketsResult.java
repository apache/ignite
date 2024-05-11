package org.shaofan.s3.model;

import javax.xml.bind.annotation.XmlRootElement;
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
