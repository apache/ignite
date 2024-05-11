package org.shaofan.s3.service.Impl;

import org.shaofan.s3.config.SystemConfig;
import org.shaofan.s3.model.*;
import org.shaofan.s3.service.S3Service;
import org.shaofan.s3.util.*;
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
        bucket.setCreationDate(DateUtil.getDateGMTFormat(new Date()));
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
    public HashMap<String, String> headObject(String bucketName, String objectKey) {
        HashMap<String, String> headInfo = new HashMap();
        String filePath = systemConfig.getDataPath() + bucketName + "/" + objectKey;
        File file = new File(filePath);
        if (file.exists()) {
            try {
                headInfo.put("Content-Disposition", "filename=" + URLEncoder.encode(file.getName(), "utf-8"));
                headInfo.put("Content-Length", FileUtil.getFileSize(file) + "");
                headInfo.put("Content-Type", FileUtil.getContentType(file.getName()));
                headInfo.put("Last-Modified", FileUtil.getLastModifyTimeGMT(file));
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
            byte[] fileByte = FileUtil.getFile(filePath);
            InputStream inputStream = new ByteArrayInputStream(fileByte);
            ;
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(fileByte.length);
            metadata.setContentType(FileUtil.getContentType(file.getName()));
            metadata.setFileName(file.getName());
            metadata.setLastModified(FileUtil.getLastModifyTime(file));
            return new S3ObjectInputStream(metadata, inputStream);
        }
        return null;
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
            ;
        } catch (Exception e) {

        }
        CompleteMultipartUploadResult complateResult = new CompleteMultipartUploadResult(bucketName, objectKey, eTag);
        return complateResult;
    }
}
