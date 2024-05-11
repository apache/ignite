package org.shaofan.s3.service.Impl;


import org.shaofan.s3.config.SystemConfig;
import org.shaofan.s3.model.*;
import org.shaofan.s3.service.DatasetPersistenceException;
import org.shaofan.s3.service.S3Service;
import org.shaofan.s3.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.*;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.util.*;

@Service
@Primary
public class S3IgfsServiceImpl implements S3Service {
    @Autowired
    @Qualifier("systemConfig")
    private SystemConfig systemConfig;    
    
    @Autowired
    private IgfsDatasetPersistenceProvider provider;
     

    @Override
    public Bucket createBucket(String bucketName) {       
        Bucket bucket = new Bucket();
        bucket.setName(bucketName);
        bucket.setCreationDate(DateUtil.getDateGMTFormat(new Date()));
        
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
    public HashMap<String, String> headObject(String bucketName, String objectKey) {
        HashMap<String, String> headInfo = new HashMap<>();
        List<S3Object> s3ObjectList = listObjects(bucketName,objectKey);        
        if (s3ObjectList.size()>0) {
            try {
            	ObjectMetadata file = s3ObjectList.get(0).getMetadata();
                headInfo.put("Content-Disposition", "filename=" + URLEncoder.encode(file.getFileName(), "utf-8"));
                headInfo.put("Content-Length", file.getContentLength() + "");
                headInfo.put("Content-Type", file.getContentType());
                headInfo.put("Last-Modified", DateUtil.getDateGMTFormat(file.getLastModified()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            headInfo.put("NoExist", "1");
        }
        return headInfo;
    }

    @Override
    public void putObject(String bucketName, String objectKey, InputStream inputStream) {
        Bucket bucket = createBucket(bucketName);
        DatasetSnapshotContext context = new StandardDatasetSnapshotContext.Builder(bucket)
        		.datasetId(objectKey)
        		.datasetName(objectKey)
        		.build();
        
        if (objectKey.endsWith("/")) {
        	provider.createBucket(bucket, objectKey);
        } else {
        	provider.createOrUpdateDatasetVersion(context, inputStream);
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
        DatasetSnapshotContext context = new StandardDatasetSnapshotContext.Builder(bucket)
        		.datasetId(objectKey)
        		.datasetName(objectKey)
        		.build();
        
        List<S3Object> metadatas = provider.getObjectsAndMetadata(bucketName, objectKey);
        
        if (metadatas.size()>0) {           
            InputStream inputStream = provider.getDatasetInputStream(context);            
            
            return new S3ObjectInputStream(metadatas.get(0).getMetadata(), inputStream);
        }
        throw new DatasetPersistenceException("Error retrieving dataset from Igfs due to: file not existed");
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(String bucketName, String objectKey) {
        createBucket(bucketName);
        InitiateMultipartUploadResult multipartUploadResult = new InitiateMultipartUploadResult();
        multipartUploadResult.setBucket(bucketName);
        multipartUploadResult.setObjectKey(objectKey);
        
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

    @Override
    public PartETag uploadPart(String bucketName, String objectKey, int partNumber, String uploadId, InputStream inputStream) {
        String tempPartFilePath = systemConfig.getTempPath() + "/" + uploadId + "/" + partNumber + ".temp";
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
            FileUtil.saveFile(partFilePath, inputStream);
            try {
                FileUtil.writeFile(tempPartFilePath, EncryptUtil.encryptByDES(partFilePath));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        String eTag = FileUtil.readFileContent(tempPartFilePath);
        PartETag partETag = new PartETag(partNumber, eTag);
        return partETag;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(String bucketName, String objectKey, String uploadId, CompleteMultipartUpload compMPU) {
       
        
        if (!uploadId.isEmpty()) {
            List<PartETag> partETagList = compMPU.getPartETags();
            boolean check = true;
            for (PartETag partETag : partETagList) {
                String tempPartFilePath = systemConfig.getTempPath() + "/" + uploadId + "/" + partETag.getPartNumber() + ".temp";
                String eTag = FileUtil.readFileContent(tempPartFilePath);
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
                	
                	Bucket bucket = new Bucket();
                 	bucket.setName(bucketName);
                    DatasetSnapshotContext context = new StandardDatasetSnapshotContext.Builder(bucket)
                     		.datasetId(objectKey)
                     		.datasetName(objectKey)                     		
                     		.build();
                     
                    
                    for (PartETag partETag : partETagList) {
                        File file = new File(EncryptUtil.decryptByDES(partETag.geteTag()));                        
                        FileInputStream sourceInputStream = new FileInputStream(file);
                        provider.appendDatasetVersion(context, sourceInputStream);
                        sourceInputStream.close();
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else {
            	throw new DatasetPersistenceException("Error save dataset from Igfs due to: partETag file not found");
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
