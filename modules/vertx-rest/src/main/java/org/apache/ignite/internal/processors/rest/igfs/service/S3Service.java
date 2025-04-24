package org.apache.ignite.internal.processors.rest.igfs.service;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.internal.processors.rest.igfs.model.*;

public interface S3Service {
    Bucket createBucket(String bucketName);

    void deleteBucket(String bucketName);

    List<Bucket> listBuckets();

    boolean headBucket(String bucketName);
    
    Boolean objectIsFolder(String bucketName, String prefix);

    List<S3Object> listObjects(String bucketName, String prefix);

    ObjectMetadata headObject(String bucketName, String objectKey);

    S3ObjectInputStream getObject(String bucketName, String objectKey);
    
    S3ObjectInputStream getObject(String bucketName, String objectKey, Range range);

    void deleteObject(String bucketName, String objectKey);

    void putObject(String bucketName, String objectKey, InputStream inputStream, Map<String, String> metaData);

    void copyObject(String sourceBucketName, String sourceObjectKey, String targetBuckName, String targetObjectKey);

    InitiateMultipartUploadResult initiateMultipartUpload(String bucketName, String objectKey,Map<String,String> userMeta);

    PartETag uploadPart(String bucketName, String objectKey, int partNumber, String uploadId, InputStream inputStream);
    
    void abortMultipartUpload(String bucketName, String objectKey,String uploadId);

    CompleteMultipartUploadResult completeMultipartUpload(String bucketName, String objectKey, String uploadId, String ownerName,CompleteMultipartUpload compMPU);
}
