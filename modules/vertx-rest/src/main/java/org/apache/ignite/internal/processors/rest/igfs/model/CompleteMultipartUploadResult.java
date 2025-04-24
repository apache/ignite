package org.apache.ignite.internal.processors.rest.igfs.model;

public class CompleteMultipartUploadResult {
    private String bucketName;
    private String objectKey;
    private String eTag;


    public CompleteMultipartUploadResult(String bucketName, String objectKey, String eTag) {
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.eTag =eTag;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public void setObjectKey(String objectKey) {
        this.objectKey = objectKey;
    }

    public String geteTag() {
        return eTag;
    }

    public void seteTag(String eTag) {
        this.eTag = eTag;
    }
}
