package org.apache.ignite.internal.processors.rest.igfs.service.Impl;

import org.apache.ignite.internal.processors.rest.igfs.config.SystemConfig;
import org.apache.ignite.internal.processors.rest.igfs.model.*;
import org.apache.ignite.internal.processors.rest.igfs.service.DatasetPersistenceException;
import org.apache.ignite.internal.processors.rest.igfs.service.S3Service;
import org.apache.ignite.internal.processors.rest.igfs.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;


import java.io.*;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.util.*;


public class S3IgfsServiceImpl implements S3Service {
    

    private SystemConfig systemConfig;    

    private IgfsDatasetPersistenceProvider provider;
    
    public S3IgfsServiceImpl(String region,SystemConfig systemConfig) {
		this.systemConfig = systemConfig;
		this.provider = new IgfsDatasetPersistenceProvider(region,systemConfig);
	}
     

    @Override
    public Bucket createBucket(String bucketName) {       
        Bucket bucket = new Bucket();
        bucket.setName(bucketName);
        bucket.setCreationDate(DateUtil.getDateIso8601Format(new Date()));
        
        return provider.createBucket(bucket, null);      
    }

    @Override
    public void deleteBucket(String bucketName) {
    	Bucket bucket = new Bucket();
        bucket.setName(bucketName);
    	DatasetSnapshotContext context = new StandardDatasetSnapshotContext.Builder(bucket).build();
    	provider.deleteAllDatasetContent(context);
    }

    @Override
    public List<Bucket> listBuckets() {
        List<Bucket> bucketList = provider.getBuckets();
        return bucketList;
    }

    @Override
    public boolean headBucket(String bucketName) {
    	Bucket bucket = new Bucket();
        bucket.setName(bucketName);
    	List<Bucket> bucketList = provider.getBuckets();
        return bucketList.contains(bucket);
    }
    
    @Override
    public Boolean objectIsFolder(String bucketName, String key) {
    	return provider.objectIsFolder(bucketName, key);
    }

    @Override
    public List<S3Object> listObjects(String bucketName, String prefix) {
        if (StringUtils.isEmpty(prefix)) {
            prefix = "";
        } else {
            if (prefix.startsWith("/")) {
                prefix = prefix.substring(1);
            }
        }
        List<S3Object> s3ObjectList = provider.getObjectsAndMetadata(bucketName,prefix);     
        return s3ObjectList;
    }

    @Override
    public ObjectMetadata headObject(String bucketName, String objectKey) {        
        ObjectMetadata metadata = provider.getObjectMetadata(bucketName, objectKey);
        return metadata;
    }

    @Override
    public void putObject(String bucketName, String objectKey, InputStream inputStream,Map<String, String> metaData) {
        Bucket bucket = createBucket(bucketName);
        DatasetSnapshotContext context = new StandardDatasetSnapshotContext.Builder(bucket)
        		.datasetId(objectKey)
        		.datasetName(objectKey)
        		.build();
        
        if (objectKey.endsWith("/")) { // create folder
        	provider.createBucket(bucket, objectKey);
        } else {
        	String md5 = provider.createOrUpdateDatasetVersion(context, inputStream);
        	if(md5!=null && !md5.isEmpty()) {
        		Map<String,String> props = metaData!=null? metaData: new HashMap<>();
        		props.put("eTag", md5);        		
                provider.setObjectMetadata(bucketName, objectKey, props);
        	}
        }
        
    }

    @Override
    public void copyObject(String sourceBucketName, String sourceObjectKey, String targetBuckName, String targetObjectKey) {
    	Bucket bucket = createBucket(targetBuckName);
        DatasetSnapshotContext context = new StandardDatasetSnapshotContext.Builder(bucket)
        		.datasetId(targetObjectKey)
        		.datasetName(targetObjectKey)
        		.build();
        
        Bucket fromBucket = new Bucket();
        fromBucket.setName(sourceBucketName);
        DatasetSnapshotContext fromContext = new StandardDatasetSnapshotContext.Builder(fromBucket)
        		.datasetId(sourceObjectKey)
        		.datasetName(sourceObjectKey)
        		.build();
        
        provider.copy(fromContext,context);
        
    }

    @Override
    public void deleteObject(String bucketName, String objectKey) {
    	Bucket bucket = new Bucket();
    	bucket.setName(bucketName);
        DatasetSnapshotContext context = new StandardDatasetSnapshotContext.Builder(bucket)
        		.datasetId(objectKey)
        		.datasetName(objectKey)
        		.build();
        
        provider.deleteDatasetContent(context);
    }

    @Override
    public S3ObjectInputStream getObject(String bucketName, String objectKey) {
    	Bucket bucket = new Bucket();
    	bucket.setName(bucketName);        
        
    	ObjectMetadata metadata = provider.getObjectMetadata(bucketName, objectKey);
       
        if (metadata!=null) {
        	DatasetSnapshotContext context = new StandardDatasetSnapshotContext.Builder(bucket)
            		.datasetId(objectKey)
            		.datasetName(objectKey)
            		.build();
            InputStream inputStream = provider.getDatasetInputStream(context);            
            
            return new S3ObjectInputStream(metadata, inputStream);
        }
        throw new DatasetPersistenceException("Error retrieving dataset from Igfs due to: file not existed");
    }

    @Override
    public S3ObjectInputStream getObject(String bucketName, String objectKey, Range range) {
    	if(range==null) {
    		return getObject(bucketName,objectKey);
    	}
    	Bucket bucket = new Bucket();
    	bucket.setName(bucketName);        
        
    	ObjectMetadata metadata = provider.getObjectMetadata(bucketName, objectKey);
       
        if (metadata!=null) {
        	DatasetSnapshotContext context = new StandardDatasetSnapshotContext.Builder(bucket)
            		.datasetId(objectKey)
            		.datasetName(objectKey)
            		.build();
        	byte[] fileByte = provider.getDatasetContent(context,range.getStart(),(int)(range.getEnd()-range.getStart()+1));    
            InputStream inputStream = new ByteArrayInputStream(fileByte);        
            metadata.setContentLength(fileByte.length);
            return new S3ObjectInputStream(metadata, inputStream);
        }
        throw new DatasetPersistenceException("Error retrieving dataset from Igfs due to: file not existed");
    }
    
    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(String bucketName, String objectKey,Map<String,String> userMeta) {
        createBucket(bucketName);
        InitiateMultipartUploadResult multipartUploadResult = new InitiateMultipartUploadResult();
        multipartUploadResult.setBucket(bucketName);
        multipartUploadResult.setObjectKey(objectKey);
        multipartUploadResult.setOwnerName(userMeta.get("ownerName"));
        
        String filePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;
        String[] filePathList = filePath.split("\\/");
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < filePathList.length - 1; i++) {
            result.append(filePathList[i]).append("/");
        }
        String fileDirPath = result.toString();
        File fileDir = new File(fileDirPath);
        if (!fileDir.exists()) {
            fileDir.mkdirs();
        }
        
        String uploadID = CommonUtil.getNewGuid();
        String tempPath = systemConfig.getTempPath() + "/" + uploadID + "/";
        File tempDir = new File(tempPath);
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        multipartUploadResult.setUploadId(uploadID);
        return multipartUploadResult;
    }
    
    public void abortMultipartUpload(String bucketName, String objectKey,String uploadId) {
    	String tempPath = systemConfig.getTempPath() + "/" + uploadId + "/";
        File tempDir = new File(tempPath);
        if (tempDir.exists() && tempPath.length()>32) {
            FileUtil.deleteDirectory(tempPath);
        }
    }

    @Override
    public PartETag uploadPart(String bucketName, String objectKey, int partNumber, String uploadId, InputStream inputStream) {
        String tempPartFilePath = systemConfig.getTempPath() + "/" + uploadId + "/" + partNumber + ".md5";
        File file = new File(tempPartFilePath);
        if (!file.exists()) {            
        	String filePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;
            String[] filePathList = filePath.split("\\/");
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < filePathList.length - 1; i++) {
                result.append(filePathList[i]).append("/");
            }
            String fileDirPath = result.toString();
            String partFilePath = fileDirPath + partNumber + ".part";
            
            try {
            	String md5 = FileUtil.saveFileWithMd5(partFilePath, inputStream);
                FileUtil.writeFile(tempPartFilePath, md5);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        String eTag = FileUtil.readFileContent(tempPartFilePath);
        PartETag partETag = new PartETag(partNumber, eTag);
        return partETag;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(String bucketName, String objectKey, String uploadId,String ownerName, CompleteMultipartUpload compMPU) {      
        if (!uploadId.isEmpty()) {
            List<PartETag> partETagList = compMPU.getPartETags();
            boolean check = true;
            for (PartETag partETag : partETagList) {
                String tempPartFilePath = systemConfig.getTempPath() + "/" + uploadId + "/" + partETag.getPartNumber() + ".md5";
                String eTag = FileUtil.readFileContent(tempPartFilePath);
                if(eTag.isEmpty()) {
                	throw new DatasetPersistenceException("Error save dataset from Igfs due to: partETag file not found");
                }
                if (!partETag.geteTag().equals(eTag)) {
                    check = false;
                    break;
                }
            }
            
            if (check) {
                partETagList.sort(new Comparator<PartETag>() {
                    @Override
                    public int compare(PartETag o1, PartETag o2) {
                        return o1.getPartNumber() - o2.getPartNumber();
                    }
                });
                try {
                	Map<String,String> props = new HashMap<>();
                	Bucket bucket = new Bucket();
                 	bucket.setName(bucketName);
                    DatasetSnapshotContext context = new StandardDatasetSnapshotContext.Builder(bucket)
                     		.datasetId(objectKey)
                     		.datasetName(objectKey)                     		
                     		.build();
                     
                    String etags = "";
                    String parts = "";
                    for (PartETag partETag : partETagList) {
                    	String filePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;
                        String[] filePathList = filePath.split("\\/");
                        StringBuilder result = new StringBuilder();
                        for (int i = 0; i < filePathList.length - 1; i++) {
                            result.append(filePathList[i]).append("/");
                        }
                        String fileDirPath = result.toString();
                        String partFilePath = fileDirPath + partETag.getPartNumber() + ".part";
                        
                        File file = new File(partFilePath);                      
                        FileInputStream sourceInputStream = new FileInputStream(file);
                        long n = provider.appendDatasetVersion(context, sourceInputStream);                        
                        sourceInputStream.close();
                        etags+=partETag.geteTag();
                        parts+=":"+n;
                    }
                    
                    etags = EncryptUtil.encryptByMD5(etags)+"-"+partETagList.size();
                    props.put("eTag", etags);
                    props.put("parts", parts);
                    props.put("usrName", ownerName);
                    provider.setObjectMetadata(bucketName, objectKey, props);
                    
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else {
            	throw new DatasetPersistenceException("Error save dataset from Igfs due to: partETag is not checked!");
            }
        }
        String eTag = "";
        try {
            eTag = EncryptUtil.encryptByMD5(bucketName + "/" + objectKey);
            
        } catch (Exception e) {
        	e.printStackTrace();
        }
        CompleteMultipartUploadResult complateResult = new CompleteMultipartUploadResult(bucketName, objectKey, eTag);
        return complateResult;
    }
}
