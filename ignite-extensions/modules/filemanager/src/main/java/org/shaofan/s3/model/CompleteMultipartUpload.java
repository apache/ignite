package org.shaofan.s3.model;

import java.util.List;

public class CompleteMultipartUpload {
    private List<PartETag> partETags;

    public CompleteMultipartUpload(List<PartETag> partETags) {
        this.partETags = partETags;
    }

    public List<PartETag> getPartETags() {
        return partETags;
    }
}
