package org.apache.ignite.internal.processors.rest.igfs.service.Impl;

import org.apache.ignite.internal.processors.rest.igfs.config.SystemConfig;
import org.apache.ignite.internal.processors.rest.igfs.model.*;
import org.apache.ignite.internal.processors.rest.igfs.service.S3Service;
import org.apache.ignite.internal.processors.rest.igfs.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.*;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.util.*;

@Service
public class S3LocalFileServiceImpl implements S3Service {
    @Autowired
    private SystemConfig systemConfig;

    @Override
    public Bucket createBucket(String bucketName) {
        String dirPath = systemConfig.getDataPath() + bucketName;
        File dir = new File(dirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        Bucket bucket = new Bucket();
        bucket.setName(bucketName);
        bucket.setCreationDate(DateUtil.getDateIso8601Format(new Date()));
        return bucket;
    }

    @Override
    public void deleteBucket(String bucketName) {
        String dirPath = systemConfig.getDataPath() + bucketName;
        FileUtil.delete(dirPath);
    }

    @Override
    public List<Bucket> listBuckets() {
        List<Bucket> bucketList = new ArrayList<>();
        String dirPath = systemConfig.getDataPath();
        File dir = new File(dirPath);
        if (dir.exists()) {
            File[] fileList = dir.listFiles();
            for (File file : fileList) {
                if (file.isDirectory()) {
                    Bucket bucket = new Bucket();
                    bucket.setName(file.getName());
                    bucket.setCreationDate(FileUtil.getCreationTime(file.getAbsoluteFile()));
                    bucketList.add(bucket);
                }
            }
        }
        return bucketList;
    }

    @Override
    public boolean headBucket(String bucketName) {
        String dirPath = systemConfig.getDataPath() + bucketName;
        File dir = new File(dirPath);
        return dir.exists();
    }
    
    @Override
    public Boolean objectIsFolder(String bucketName, String objectKey) {
    	String filePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;    	
        File dir = new File(filePath);
        if(dir.exists()) {
        	return dir.isDirectory();
        }
        return null;
    }

    @Override
    public List<S3Object> listObjects(String bucketName, String prefix) {
        if (StringUtils.isEmpty(prefix)) {
            prefix = "/";
        } else {
            if (!prefix.startsWith("/")) {
                prefix = "/" + prefix;
            }
        }
        List<S3Object> s3ObjectList = new ArrayList<>();
        String dirPath = systemConfig.getDataPath() + bucketName + prefix;
        File dir = new File(dirPath);
        File[] fileList = dir.listFiles();
        for (File file : fileList) {
            S3Object s3Object = new S3Object();
            s3Object.setBucketName(bucketName);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setFileName(file.getName());
            if (!file.isDirectory()) {
                s3Object.setKey(file.getName());
                objectMetadata.setContentLength(FileUtil.getFileSize(file));
                objectMetadata.setContentType(FileUtil.getContentType(file.getName()));
                objectMetadata.setLastModified(FileUtil.getLastModifyTime(file));
            } else {
                s3Object.setKey(file.getName() + "/");
            }
            s3Object.setMetadata(objectMetadata);
            s3ObjectList.add(s3Object);
        }
        return s3ObjectList;
    }

    @Override
    public ObjectMetadata headObject(String bucketName, String objectKey) {
    	ObjectMetadata metadata = new ObjectMetadata();
        String filePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;
        File file = new File(filePath);
        if (file.exists()) {
            try {               
                
            	metadata.setFileName(file.getName());
            	metadata.setContentLength(FileUtil.getFileSize(file));
            	metadata.setContentType(FileUtil.getContentType(file.getName()));
            	metadata.setLastModified(FileUtil.getLastModifyTime(file));
                
                String eTag = EncryptUtil.encryptByMD5(bucketName+"/"+objectKey);
                metadata.setETag(eTag+'-'+0);    			
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            return null;
        }
        return metadata;
    }

    @Override
    public void putObject(String bucketName, String objectKey, InputStream inputStream,Map<String, String> metaData) {
        createBucket(bucketName);
        String filePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;
        if (filePath.endsWith("/")) {
            File fileDir = new File(filePath);
            if (!fileDir.exists()) {
                fileDir.mkdirs();
            }
        } else {
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
            FileUtil.saveFile(filePath, inputStream);
        }        
    }

    @Override
    public void copyObject(String sourceBucketName, String sourceObjectKey, String targetBuckName, String targetObjectKey) {
        String sourceFilePath = systemConfig.getDataPath() + sourceBucketName + "/" + sourceObjectKey;
        createBucket(targetBuckName);
        String targetFilePath = systemConfig.getDataPath() + targetBuckName + "/" + targetObjectKey;
        String[] filePathList = targetFilePath.split("\\/");
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < filePathList.length - 1; i++) {
            result.append(filePathList[i]).append("/");
        }
        String fileDirPath = result.toString();
        File fileDir = new File(fileDirPath);
        if (!fileDir.exists()) {
            fileDir.mkdirs();
        }
        try{
            FileUtil.copyFile(sourceFilePath,targetFilePath);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void deleteObject(String bucketName, String objectKey) {
        String filePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;
        FileUtil.delete(filePath);
    }

    @Override
    public S3ObjectInputStream getObject(String bucketName, String objectKey) {
        String filePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;
        File file = new File(filePath);
        if (file.exists()) {            
            InputStream inputStream = FileUtil.getFileStream(filePath);
            
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(FileUtil.getFileSize(file));
            metadata.setContentType(FileUtil.getContentType(file.getName()));
            metadata.setFileName(file.getName());
            metadata.setLastModified(FileUtil.getLastModifyTime(file));
            String eTag = EncryptUtil.encryptByMD5(bucketName+"/"+objectKey);
            metadata.setETag(eTag+'-'+0);    
            return new S3ObjectInputStream(metadata, inputStream);
        }
        return null;
    }
    
    @Override
    public S3ObjectInputStream getObject(String bucketName, String objectKey, Range range) {
    	if(range==null) {
    		return getObject(bucketName,objectKey);
    	}
        String filePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;
        File file = new File(filePath);
        if (file.exists()) {
            byte[] fileByte = FileUtil.getFile(filePath,range.getStart(),(int)(range.getEnd()-range.getStart()+1));
            InputStream inputStream = new ByteArrayInputStream(fileByte);
            
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(fileByte.length);
            metadata.setContentType(FileUtil.getContentType(file.getName()));
            metadata.setFileName(file.getName());
            metadata.setLastModified(FileUtil.getLastModifyTime(file));
            String eTag = EncryptUtil.encryptByMD5(bucketName+"/"+objectKey);
            metadata.setETag(eTag+'-'+0);    
            return new S3ObjectInputStream(metadata, inputStream);
        }
        return null;
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(String bucketName, String objectKey,Map<String, String> metaData) {
        createBucket(bucketName);
        InitiateMultipartUploadResult multipartUploadResult = new InitiateMultipartUploadResult();
        multipartUploadResult.setBucket(bucketName);
        multipartUploadResult.setObjectKey(objectKey);
        multipartUploadResult.setOwnerName(metaData.get("ownerName"));
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
    public CompleteMultipartUploadResult completeMultipartUpload(String bucketName, String objectKey, String uploadId, String ownerName,CompleteMultipartUpload compMPU) {
        String merageFilePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;
        File merageFile = new File(merageFilePath);
        if (!merageFile.exists()) {
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
                    BufferedOutputStream destOutputStream = new BufferedOutputStream(new FileOutputStream(merageFilePath));
                    for (PartETag partETag : partETagList) {
                        File file = new File(EncryptUtil.decryptByDES(partETag.geteTag()));
                        byte[] fileBuffer = new byte[1024 * 1024 * 5];
                        int readBytesLength = 0;
                        BufferedInputStream sourceInputStream = new BufferedInputStream(new FileInputStream(file));
                        while ((readBytesLength = sourceInputStream.read(fileBuffer)) != -1) {
                            destOutputStream.write(fileBuffer, 0, readBytesLength);
                        }
                        sourceInputStream.close();
                    }
                    destOutputStream.flush();
                    destOutputStream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
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
