package org.shaofan.s3.util;

import org.shaofan.s3.config.SystemConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class S3Util {
    @Autowired
    private SystemConfig systemConfig;
    
    private S3Client s3;

    private S3Client getClient() {
    	if(s3!=null ) {
    		return s3;
    	}
    	String endpoint = systemConfig.getEndpointOverride();
    	if(endpoint==null || endpoint.isBlank()) {
    		endpoint = CommonUtil.getApiPath() + "s3/";
    	}
        s3 = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(systemConfig.getAccessKey(), systemConfig.getSecretAccessKey())))
                .endpointOverride(URI.create(endpoint))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).chunkedEncodingEnabled(false).build())
                .region(Region.AWS_CN_GLOBAL)
                .build();
        return s3;
    }

    private S3Presigner getPresigner() {
    	String endpoint = systemConfig.getEndpointOverride();
    	if(endpoint==null || endpoint.isBlank()) {
    		endpoint = CommonUtil.getApiPath() + "s3/";
    	}
    	
        S3Presigner s3Presigner = S3Presigner.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(systemConfig.getAccessKey(), systemConfig.getSecretAccessKey())))
                .endpointOverride(URI.create(endpoint))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).chunkedEncodingEnabled(false).build())
                .region(Region.AWS_CN_GLOBAL)
                .build();
        return s3Presigner;
    }

    
    public void close() {
    	if(s3!=null ) {
    		s3.close();
    		s3 = null;
    	}
    }
    
    public void createBucket(String bucketName) {
        S3Client s3Client = getClient();
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucketName)
                .build();
        s3Client.createBucket(request);
        
    }

    public List<Bucket> getBucketList() {
        S3Client s3Client = getClient();
        ListBucketsRequest request = ListBucketsRequest.builder().build();
        ListBucketsResponse response = s3Client.listBuckets(request);
        List<Bucket> bucketList = response.buckets();
        
        return bucketList;
    }

    public boolean headBucket(String bucketName) {
        S3Client s3Client = getClient();
        boolean checkExist = true;
        try {
            HeadBucketRequest request = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            HeadBucketResponse response = s3Client.headBucket(request);
        } catch (NoSuchBucketException e) {
            checkExist = false;
        }
        
        return checkExist;
    }

    public void deleteBucket(String bucketName) {
        S3Client s3Client = getClient();
        DeleteBucketRequest request = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        s3Client.deleteBucket(request);
        
    }


    public List<S3Object> getObjectList(String bucketName, String prefix) {
        S3Client s3Client = getClient();
        ListObjectsRequest request = ListObjectsRequest.builder().bucket(bucketName).prefix(prefix).delimiter("/").build();
        ListObjectsResponse response = s3Client.listObjects(request);
        List<S3Object> s3ObjectList = response.contents();
        
        return s3ObjectList;
    }

    public HashMap<String, String> headObject(String bucketName, String key) {
        HashMap<String, String> headInfo = new HashMap<>();
        S3Client s3Client = getClient();
        try {
            HeadObjectRequest objectRequest = HeadObjectRequest.builder()
                    .key(key)
                    .bucket(bucketName)
                    .build();
            HeadObjectResponse objectHead = s3Client.headObject(objectRequest);
            
            headInfo.put("contentType", objectHead.contentType());
            headInfo.put("contentLength", objectHead.contentLength() + "");
            headInfo.put("contentDisposition", objectHead.contentDisposition());
            headInfo.put("lastModified", objectHead.lastModified().toString());
        } catch (NoSuchKeyException e) {
            headInfo.put("noExist", "1");
        }
        return headInfo;
    }
    
    
    public String putObjectACL(String bucketName, String key, String acl, AccessControlPolicy policy) {
        
        S3Client s3Client = getClient();
        try {
        	
        	PutObjectAclRequest objectRequest = PutObjectAclRequest.builder()
                    .key(key)
                    .bucket(bucketName)
                    .acl(acl)
                    .accessControlPolicy(policy)
                    .build();
            PutObjectAclResponse objectHead = s3Client.putObjectAcl(objectRequest);
            
            return objectHead.requestChargedAsString();
            
        } catch (NoSuchKeyException e) {
            return e.getMessage();
        }        
    }
    
    public List<Grant> getObjectACL(String bucketName, String key) {        
        S3Client s3Client = getClient();
        try {
        	
        	GetObjectAclRequest objectRequest = GetObjectAclRequest.builder()
                    .key(key)
                    .bucket(bucketName)             
                    .build();
            GetObjectAclResponse objectHead = s3Client.getObjectAcl(objectRequest);
                        
            return objectHead.grants();
            
        } catch (NoSuchKeyException e) {
            return null;
        }        
    }

    public void upload(String bucketName, String key, InputStream inputStream) throws Exception {
        S3Client s3Client = getClient();
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
        RequestBody requestBody = RequestBody.fromBytes(FileUtil.convertStreamToByte(inputStream));
        s3Client.putObject(request, requestBody);
        
    }

    public byte[] getFileByte(String bucketName, String key) {
        S3Client s3Client = getClient();
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(request);
        byte[] data = objectBytes.asByteArray();
        
        return data;
    }
    
    public InputStream getFileInputStream(String bucketName, String key) {
        S3Client s3Client = getClient();
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(request);       
        return stream;
    }

    public String getDownLoadUrl(String bucketName, String key) {
        S3Presigner s3Presigner = getPresigner();
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
        GetObjectPresignRequest getObjectPresignRequest = GetObjectPresignRequest.builder().signatureDuration(Duration.ofMinutes(5)).getObjectRequest(getObjectRequest).build();
        PresignedGetObjectRequest presignedGetObjectRequest = s3Presigner.presignGetObject(getObjectPresignRequest);
        String url = presignedGetObjectRequest.url().toString();
       
        s3Presigner.close();
        return url;
    }

    public void delete(String bucketName, String key) {
        S3Client s3Client = getClient();
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        s3Client.deleteObject(request);
        
    }

    public void copyObject(String sourceBucketName, String sourceKey, String targetBucketName, String targetKey) throws Exception {
        S3Client s3Client = getClient();
        CopyObjectRequest request = CopyObjectRequest.builder()
                .sourceBucket(sourceBucketName)
                .sourceKey(sourceKey)
                .destinationBucket(targetBucketName)
                .destinationKey(targetKey).build();
        s3Client.copyObject(request);
        
    }

    public String createMultipartUpload(String bucketName, String key) {
        String uploadID = "";
        S3Client s3Client = getClient();
        CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key).build();
        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(request);
        uploadID = response.uploadId();
        
        return uploadID;
    }

    public String uploadPart(String bucketName, String key, String uploadID, int partNumber, InputStream inputStream) {
        String eTag = "";
        S3Client s3Client = getClient();
        try {
            byte[] partData = FileUtil.convertStreamToByte(inputStream);
            UploadPartRequest request = UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadID)
                    .partNumber(partNumber)
                    .contentLength(Long.parseLong(partData.length + ""))
                    .build();
            UploadPartResponse response = s3Client.uploadPart(request, RequestBody.fromBytes(partData));
            eTag = response.eTag();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return eTag;
    }

    public String completeMultipartUpload(String bucketName, String key, String uploadID, List<CompletedPart> partList) {
        S3Client s3Client = getClient();
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadID)
                .multipartUpload(CompletedMultipartUpload.builder().parts(partList).build())
                .build();
        CompleteMultipartUploadResponse response = s3Client.completeMultipartUpload(request);
        
        return response.eTag();
    }
}
