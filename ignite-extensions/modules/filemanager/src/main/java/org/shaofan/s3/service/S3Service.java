package org.shaofan.s3.service;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

import org.shaofan.s3.model.*;

public interface S3Service {
    Bucket createBucket(String bucketName);

    void deleteBucket(String bucketName);

    List<Bucket> listBuckets();

    boolean headBucket(String bucketName);

    List<S3Object> listObjects(String bucketName, String prefix);

    HashMap<String, String> headObject(String bucketName, String objectKey);

    S3ObjectInputStream getObject(String bucketName, String objectKey);

    void deleteObject(String bucketName, String objectKey);

    void putObject(String bucketName, String objectKey, InputStream inputStream);

    void copyObject(String sourceBucketName, String sourceObjectKey, String targetBuckName, String targetObjectKey);

    InitiateMultipartUploadResult initiateMultipartUpload(String bucketName, String objectKey);

    PartETag uploadPart(String bucketName, String objectKey, int partNumber, String uploadId, InputStream inputStream);

    CompleteMultipartUploadResult completeMultipartUpload(String bucketName, String objectKey, String uploadId, CompleteMultipartUpload compMPU);
}
