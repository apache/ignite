package org.apache.ignite.internal.processors.rest.igfs.model;

import java.io.InputStream;

public class S3Object {
	
    private String bucketName;
    private String key;
    private ObjectMetadata metadata;
    private InputStream objectData;

    public S3Object(){

    }

    public S3Object(String bucketName, String key, ObjectMetadata metadata, InputStream objectData) {
        this.bucketName = bucketName;
        this.key = key;
        this.metadata = metadata;
        this.objectData = objectData;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public ObjectMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(ObjectMetadata metadata) {
        this.metadata = metadata;
    }

    public InputStream getObjectData() {
        return objectData;
    }

    public void setObjectData(InputStream objectData) {
        this.objectData = objectData;
    }
}
