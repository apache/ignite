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

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketAccelerateConfiguration;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketReplicationConfiguration;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketPolicyRequest;
import com.amazonaws.services.s3.model.DeleteBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectTaggingRequest;
import com.amazonaws.services.s3.model.DeleteObjectTaggingResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketAclRequest;
import com.amazonaws.services.s3.model.GetBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyRequest;
import com.amazonaws.services.s3.model.GetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectAclRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.GetS3AccountOwnerRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListBucketAnalyticsConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketAnalyticsConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketInventoryConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketInventoryConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketMetricsConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketMetricsConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfVersionsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.SetBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketPolicyRequest;
import com.amazonaws.services.s3.model.SetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingResult;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.waiters.AmazonS3Waiters;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class to simulate the functionality of {@link AmazonS3Client}.
 */
public final class DummyS3Client extends AmazonS3Client {
    /** Map of Bucket names as keys and the keys as set of values. */
    private final Map<String, Set<String>> objMap;

    /**
     * Constructor.
     */
    public DummyS3Client() {
        this.objMap = new HashMap<>();
    }

    /**
     * Constructor to add an object map with fake data.
     */
    public DummyS3Client(Map<String, Set<String>> objMap) {
        this.objMap = Objects.requireNonNull(objMap, "Object map cannot be null");
    }

    /** Empty Method. */
    @Override public void setEndpoint(String endpoint) {
    }

    /** Unsupported Operation. */
    @Override public void setRegion(Region region) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setS3ClientOptions(S3ClientOptions clientOptions) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageCls)
        throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation)
        throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** {@inheritDoc} */
    @Override public ObjectListing listObjects(String bucketName) throws SdkClientException {
        checkBucketExists(bucketName);
        return DummyObjectListing.of(bucketName, objMap.get(bucketName));
    }

    /** {@inheritDoc} */
    @Override public ObjectListing listObjects(String bucketName, String prefix) throws SdkClientException {
        checkBucketExists(bucketName);

        Set<String> keys = objMap.get(bucketName).stream()
            .filter(key -> key.startsWith(prefix)).collect(Collectors.toSet());

        return DummyObjectListing.of(bucketName, keys);
    }

    /** Unsupported Operation. */
    @Override public ObjectListing listObjects(ListObjectsRequest listObjectsReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public ListObjectsV2Result listObjectsV2(String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public ListObjectsV2Result listObjectsV2(String bucketName,
        String prefix) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Req) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** {@inheritDoc} */
    @Override public ObjectListing listNextBatchOfObjects(ObjectListing previousObjListing) throws SdkClientException {
        return previousObjListing;
    }

    /** Unsupported Operation. */
    @Override public ObjectListing listNextBatchOfObjects(ListNextBatchOfObjectsRequest listNextBatchOfObjectsReq)
        throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public VersionListing listVersions(String bucketName, String prefix) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public VersionListing listNextBatchOfVersions(VersionListing previousVerListing) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public VersionListing listNextBatchOfVersions(ListNextBatchOfVersionsRequest listNextBatchOfVersionsReq)
        throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String verIdMarker,
        String delim, Integer maxResults) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public VersionListing listVersions(ListVersionsRequest listVersionsReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public Owner getS3AccountOwner() throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public Owner getS3AccountOwner(GetS3AccountOwnerRequest getS3AccountOwnerReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** {@inheritDoc} */
    @Override public boolean doesBucketExist(String bucketName) throws SdkClientException {
        return objMap.containsKey(bucketName);
    }

    /** Unsupported Operation. */
    @Override public HeadBucketResult headBucket(HeadBucketRequest headBucketReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public List<Bucket> listBuckets() throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public List<Bucket> listBuckets(ListBucketsRequest listBucketsReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public String getBucketLocation(String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public String getBucketLocation(GetBucketLocationRequest getBucketLocationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public Bucket createBucket(CreateBucketRequest createBucketReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** {@inheritDoc} */
    @Override public Bucket createBucket(String bucketName) throws SdkClientException {
        if (doesBucketExist(bucketName))
            throw new AmazonS3Exception("The specified bucket already exist");
        else {
            objMap.put(bucketName, new HashSet<>());

            return new Bucket();
        }
    }

    /** Unsupported Operation. */
    @Override public Bucket createBucket(String bucketName,
        com.amazonaws.services.s3.model.Region region) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public Bucket createBucket(String bucketName, String region) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public AccessControlList getObjectAcl(String bucketName,
        String key) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public AccessControlList getObjectAcl(String bucketName, String key,
        String verId) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public AccessControlList getObjectAcl(GetObjectAclRequest getObjAclReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setObjectAcl(String bucketName, String key, AccessControlList acl) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setObjectAcl(String bucketName, String key,
        CannedAccessControlList acl) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setObjectAcl(String bucketName, String key, String verId,
        AccessControlList acl) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setObjectAcl(String bucketName, String key, String verId,
        CannedAccessControlList acl) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setObjectAcl(SetObjectAclRequest setObjAclReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public AccessControlList getBucketAcl(String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketAcl(SetBucketAclRequest setBucketAclReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketAcl(String bucketName, AccessControlList acl) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketAcl(String bucketName, CannedAccessControlList acl) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public ObjectMetadata getObjectMetadata(String bucketName, String key) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public ObjectMetadata getObjectMetadata(
        GetObjectMetadataRequest getObjMetadataReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public S3Object getObject(String bucketName, String key) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public S3Object getObject(GetObjectRequest getObjReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public ObjectMetadata getObject(GetObjectRequest getObjReq, File destFile) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public String getObjectAsString(String bucketName, String key) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public GetObjectTaggingResult getObjectTagging(GetObjectTaggingRequest getObjTaggingReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public SetObjectTaggingResult setObjectTagging(SetObjectTaggingRequest setObjTaggingReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public DeleteObjectTaggingResult deleteObjectTagging(DeleteObjectTaggingRequest delObjTaggingReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucket(DeleteBucketRequest delBucketReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucket(String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public PutObjectResult putObject(PutObjectRequest putObjReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public PutObjectResult putObject(String bucketName, String key,
        File file) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** {@inheritDoc} */
    @Override public PutObjectResult putObject(String bucketName, String key, InputStream input,
        ObjectMetadata metadata) throws SdkClientException {
        checkBucketExists(bucketName);

        Set<String> keys = objMap.get(bucketName);

        keys.add(key);

        return new PutObjectResult();
    }

    /** Unsupported Operation. */
    @Override public PutObjectResult putObject(String bucketName, String key,
        String content) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public CopyObjectResult copyObject(String srcBucketName, String srcKey, String destBucketName,
        String destKey) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public CopyObjectResult copyObject(CopyObjectRequest cpObjReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public CopyPartResult copyPart(CopyPartRequest cpPartReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** {@inheritDoc} */
    @Override public void deleteObject(String bucketName, String key) throws SdkClientException {
        checkBucketExists(bucketName);
        Set<String> keys = objMap.get(bucketName);

        Set<String> keysToDel = keys.stream().filter(k -> k.contains(key)).collect(Collectors.toSet());

        keys.removeAll(keysToDel);

        objMap.put(bucketName, keys);
    }

    /** Unsupported Operation. */
    @Override public void deleteObject(DeleteObjectRequest delObjReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public DeleteObjectsResult deleteObjects(DeleteObjectsRequest delObjectsReq)
        throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteVersion(String bucketName, String key, String verId) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteVersion(DeleteVersionRequest delVerReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketLoggingConfiguration getBucketLoggingConfiguration(
        String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketLoggingConfiguration getBucketLoggingConfiguration(
        GetBucketLoggingConfigurationRequest getBucketLoggingConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketLoggingConfiguration(
        SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketVersioningConfiguration getBucketVersioningConfiguration(
        String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketVersioningConfiguration getBucketVersioningConfiguration(
        GetBucketVersioningConfigurationRequest getBucketVersioningConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketVersioningConfiguration(
        SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketLifecycleConfiguration getBucketLifecycleConfiguration(
        GetBucketLifecycleConfigurationRequest getBucketLifecycleConfigurationReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketLifecycleConfiguration(String bucketName,
        BucketLifecycleConfiguration bucketLifecycleConfiguration) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketLifecycleConfiguration(
        SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketLifecycleConfiguration(String bucketName) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketLifecycleConfiguration(
        DeleteBucketLifecycleConfigurationRequest delBucketLifecycleConfigurationReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(
        GetBucketCrossOriginConfigurationRequest getBucketCrossOriginConfigurationReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketCrossOriginConfiguration(String bucketName,
        BucketCrossOriginConfiguration bucketCrossOriginConfiguration) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketCrossOriginConfiguration(
        SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketCrossOriginConfiguration(String bucketName) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketCrossOriginConfiguration(
        DeleteBucketCrossOriginConfigurationRequest delBucketCrossOriginConfigurationReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketTaggingConfiguration getBucketTaggingConfiguration(
        GetBucketTaggingConfigurationRequest getBucketTaggingConfigurationReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketTaggingConfiguration(String bucketName,
        BucketTaggingConfiguration bucketTaggingConfiguration) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketTaggingConfiguration(
        SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketTaggingConfiguration(String bucketName) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketTaggingConfiguration(
        DeleteBucketTaggingConfigurationRequest delBucketTaggingConfigurationReq) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketNotificationConfiguration getBucketNotificationConfiguration(
        String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketNotificationConfiguration getBucketNotificationConfiguration(
        GetBucketNotificationConfigurationRequest getBucketNotificationConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketNotificationConfiguration(
        SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketNotificationConfiguration(String bucketName,
        BucketNotificationConfiguration bucketNotificationConfiguration) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketWebsiteConfiguration getBucketWebsiteConfiguration(
        String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketWebsiteConfiguration getBucketWebsiteConfiguration(
        GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketWebsiteConfiguration(String bucketName,
        BucketWebsiteConfiguration configuration) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketWebsiteConfiguration(
        SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketWebsiteConfiguration(String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketWebsiteConfiguration(
        DeleteBucketWebsiteConfigurationRequest delBucketWebsiteConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketPolicy getBucketPolicy(String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPlcReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketPolicy(String bucketName, String plcText) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketPolicy(SetBucketPolicyRequest setBucketPlcReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketPolicy(String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketPolicy(DeleteBucketPolicyRequest delBucketPlcReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public URL generatePresignedUrl(String bucketName, String key,
        Date expiration) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public URL generatePresignedUrl(String bucketName, String key, Date expiration,
        HttpMethod mtd) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public URL generatePresignedUrl(
        GeneratePresignedUrlRequest generatePresignedUrlReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public InitiateMultipartUploadResult initiateMultipartUpload(
        InitiateMultipartUploadRequest req) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public UploadPartResult uploadPart(UploadPartRequest req) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public PartListing listParts(ListPartsRequest req) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void abortMultipartUpload(
        AbortMultipartUploadRequest req) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public CompleteMultipartUploadResult completeMultipartUpload(
        CompleteMultipartUploadRequest req) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public MultipartUploadListing listMultipartUploads(
        ListMultipartUploadsRequest req) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest req) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void restoreObject(RestoreObjectRequest req) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void restoreObject(String bucketName, String key, int expirationInDays) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void enableRequesterPays(String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void disableRequesterPays(String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public boolean isRequesterPaysEnabled(String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketReplicationConfiguration(String bucketName,
        BucketReplicationConfiguration configuration) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketReplicationConfiguration(
        SetBucketReplicationConfigurationRequest setBucketReplicationConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketReplicationConfiguration getBucketReplicationConfiguration(
        String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketReplicationConfiguration getBucketReplicationConfiguration(
        GetBucketReplicationConfigurationRequest getBucketReplicationConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketReplicationConfiguration(
        String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void deleteBucketReplicationConfiguration(
        DeleteBucketReplicationConfigurationRequest req) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public boolean doesObjectExist(String bucketName,
        String objName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketAccelerateConfiguration getBucketAccelerateConfiguration(
        String bucketName) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public BucketAccelerateConfiguration getBucketAccelerateConfiguration(
        GetBucketAccelerateConfigurationRequest getBucketAccelerateConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketAccelerateConfiguration(String bucketName,
        BucketAccelerateConfiguration accelerateConfiguration) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public void setBucketAccelerateConfiguration(
        SetBucketAccelerateConfigurationRequest setBucketAccelerateConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(String bucketName,
        String id) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(
        DeleteBucketMetricsConfigurationRequest delBucketMetricsConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(String bucketName,
        String id) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(
        GetBucketMetricsConfigurationRequest getBucketMetricsConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(String bucketName,
        MetricsConfiguration metricsConfiguration) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(
        SetBucketMetricsConfigurationRequest setBucketMetricsConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public ListBucketMetricsConfigurationsResult listBucketMetricsConfigurations(
        ListBucketMetricsConfigurationsRequest listBucketMetricsConfigurationsReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(String bucketName,
        String id) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(
        DeleteBucketAnalyticsConfigurationRequest delBucketAnalyticsConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(String bucketName,
        String id) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(
        GetBucketAnalyticsConfigurationRequest getBucketAnalyticsConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(String bucketName,
        AnalyticsConfiguration analyticsConfiguration) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(
        SetBucketAnalyticsConfigurationRequest setBucketAnalyticsConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public ListBucketAnalyticsConfigurationsResult listBucketAnalyticsConfigurations(
        ListBucketAnalyticsConfigurationsRequest listBucketAnalyticsConfigurationsReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(String bucketName,
        String id) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(
        DeleteBucketInventoryConfigurationRequest delBucketInventoryConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(String bucketName,
        String id) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(
        GetBucketInventoryConfigurationRequest getBucketInventoryConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(String bucketName,
        InventoryConfiguration inventoryConfiguration) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(
        SetBucketInventoryConfigurationRequest setBucketInventoryConfigurationReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public ListBucketInventoryConfigurationsResult listBucketInventoryConfigurations(
        ListBucketInventoryConfigurationsRequest listBucketInventoryConfigurationsReq) throws SdkClientException {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public com.amazonaws.services.s3.model.Region getRegion() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public String getRegionName() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public URL getUrl(String bucketName, String key) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /** Unsupported Operation. */
    @Override public AmazonS3Waiters waiters() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    /**
     * Check if a bucket exists.
     *
     * @param bucketName bucket name to check.
     * @throws AmazonS3Exception If the specified bucket does not exist.
     */
    private void checkBucketExists(String bucketName) {
        if (!doesBucketExist(bucketName))
            throw new AmazonS3Exception("The specified bucket does not exist");
    }
}
