/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.rest.igfs.service.Impl;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.rest.igfs.config.SystemConfig;
import org.apache.ignite.internal.processors.rest.igfs.model.Bucket;
import org.apache.ignite.internal.processors.rest.igfs.model.DatasetSnapshotContext;
import org.apache.ignite.internal.processors.rest.igfs.model.ObjectMetadata;
import org.apache.ignite.internal.processors.rest.igfs.model.S3Object;
import org.apache.ignite.internal.processors.rest.igfs.service.DatasetPersistenceException;
import org.apache.ignite.internal.processors.rest.igfs.util.DateUtil;
import org.apache.ignite.internal.processors.rest.igfs.util.EncryptUtil;
import org.apache.ignite.internal.processors.rest.igfs.util.FileUtil;
import org.apache.ignite.internal.processors.rest.igfs.util.IgfsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * An {@link DatasetPersistenceProvider} that uses AWS Igfs for storage.
 */
public class IgfsDatasetPersistenceProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgfsDatasetPersistenceProvider.class);
    
    public static final String BUCKET_NAME_PROP = "Bucket Name";
    public static final String KEY_PREFIX_PROP = "Key Prefix";
    public static final String CREDENTIALS_PROVIDER_PROP = "Credentials Provider";
    public static final String ACCESS_KEY_PROP = "Access Key";
    public static final String SECRET_ACCESS_KEY_PROP = "Secret Access Key";
    public static final String ENDPOINT_URL_PROP = "Endpoint URL";

    public enum CredentialProvider {
        STATIC,
        DEFAULT_CHAIN
    }

    private Map<String,IgniteFileSystem> fsMap = new HashMap<>();
    private Map<String,String> regionMap = new HashMap<>();
    
    private IgniteFileSystem igfs;
    
    // default bucket container
    private String s3BucketName = "igfs";
    
    private String region;

    private SystemConfig systemConfig;
    
    public IgfsDatasetPersistenceProvider(String region,SystemConfig systemConfig) {
    	this.region = region;
    	this.systemConfig = systemConfig;
    }
    
   
    /**
     *  获取fs，使用单一的fs存储所有的buckets
     * @param bucketName
     * @return
     */
    public IgniteFileSystem fs(String bucketName) {
    	IgniteFileSystem globalIgfs = allFS().get(bucketName);    	
    	if(globalIgfs!=null) {
    		return globalIgfs;
    	}
    	
    	if(igfs!=null) {
    		return igfs;
    	}
    	
    	throw new IllegalArgumentException("The system igfs for storage bucket '" + bucketName + "' not existed");
    }
    
    public Map<String,IgniteFileSystem> allFS(){
    	if(fsMap.isEmpty()) {
    		s3BucketName = systemConfig.getS3BucketName();
            if (!StringUtils.hasText(s3BucketName)) {
                throw new IllegalArgumentException("The property '" + BUCKET_NAME_PROP + "' must be provided");
            }
            
            if(true) {
	            Ignite ignite = Ignition.ignite(region);
	            for(IgniteFileSystem fs: ignite.fileSystems()) {
	            	if(igfs==null) {
						igfs = fs; // default storage is first fs.
					}
					String bucket = fs.name();
					fsMap.put(bucket, fs);
					regionMap.put(bucket, ignite.name());			
					if(bucket.equals(s3BucketName)) {
						igfs = fs; // default storage
					}
				}
            }
            
            if(true) {           
	            
	            for(Ignite ignite: Ignition.allGrids()) {
	    			for(IgniteFileSystem fs: ignite.fileSystems()) {
	    				if(StringUtils.pathEquals(ignite.name(),region)) {
	    					continue;
	    				}
	    				if(StringUtils.hasText(ignite.name())) {
		    				String prefix = ignite.name()+"-"+fs.name();
	    					fsMap.put(prefix, fs);
	    					regionMap.put(prefix, ignite.name());
	    				}
	    				else if(!fsMap.containsKey(fs.name())) {
	    					String bucket = fs.name();
	    					fsMap.put(bucket, fs);
	    					regionMap.put(bucket, ignite.name());  
	    				}	    				
	    			}
	    		}
            }
    	}
    	return fsMap;
    }
   
    public List<Bucket> getBuckets() {
		List<Bucket> buckets = new ArrayList<>();
		// 系统保留的bucket		
        for(Map.Entry<String,IgniteFileSystem> fsEnt : allFS().entrySet()) {
            try {
            	IgniteFileSystem fs = fsEnt.getValue();
            	
            	String fsName = fsEnt.getKey();
            	String region = regionMap.get(fsName);
                Bucket bucket = new Bucket();
				bucket.setName(fsName);
				bucket.setCreationDate(DateUtil.getDateIso8601Format(new Date(DateUtil.cpuStartTime)));
				bucket.setAuthor(fsName);
				bucket.setRegion(region);
                buckets.add(bucket);
                
            } catch (Exception ex) {
            	ex.printStackTrace();
            }
        }
        
        if(igfs==null) {
        	return buckets;
        }
        // 用户新建的bucket
        IgniteFileSystem fs = fs("/");
		IgfsPath root = new IgfsPath("/");
		Collection<IgfsFile> list = fs.listFiles(root);
		for(IgfsFile b: list) {
			if(b.isDirectory() && !allFS().containsKey(b.path().toString().substring(1))) {
				Bucket bucket = new Bucket();
				bucket.setName(b.path().toString().substring(1));
				bucket.setCreationDate(DateUtil.getDateIso8601Format(new Date(b.modificationTime())));
				bucket.setAuthor(fs.name());
				bucket.setRegion(region);
				buckets.add(bucket);
			}
		}
		return buckets;
	}
    
    public Bucket createBucket(Bucket bucket,String path) {    	
    	if(allFS().containsKey(bucket.getName())){
    		return bucket;
    	}
    	IgniteFileSystem fs = fs(bucket.getName());
        IgfsPath dir = new IgfsPath("/"+bucket.getName());
        try {
        	IgfsUtils.mkdirs(fs,dir);
        	if(path!=null && !path.isBlank()) {
        		IgfsPath sub = new IgfsPath(dir,path);
        		IgfsUtils.mkdirs(fs,sub);        		
        	}
        	bucket.setAuthor(fs.name());
        	bucket.setRegion(region);
            LOGGER.debug("Successfully saved Igfs '{}' with bucket '{}'", new Object[]{s3BucketName, bucket.getName()});
        } catch (Exception e) {
            throw new DatasetPersistenceException("Error saving dataset version to Igfs due to: " + e.getMessage(), e);
        }
    	return bucket;
    }

    
    public void saveDatasetContent(final DatasetSnapshotContext context, byte[] contentStream) throws DatasetPersistenceException {
        createOrUpdateDatasetVersion(context, contentStream);
    }

    
    public void updateDatasetVersion(final DatasetSnapshotContext context, byte[] contentStream) throws DatasetPersistenceException {
        createOrUpdateDatasetVersion(context, contentStream);
    }

    private void createOrUpdateDatasetVersion(final DatasetSnapshotContext context, byte[] contentStream)
            throws DatasetPersistenceException {
        final String key = getKey(context);
        final String dir = getKeyPrefix(context.getBucketName(),context.getDatasetName());
        LOGGER.debug("Saving dataset version to igfs in bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        IgniteFileSystem fs = fs(context.getBucketName());
        IgfsPath datasetFile = new IgfsPath(key);
        try {
        	IgfsUtils.mkdirs(fs,new IgfsPath(dir));
        	IgfsUtils.create(fs,datasetFile, contentStream);
            LOGGER.debug("Successfully saved dataset version to Igfs bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        } catch (Exception e) {
            throw new DatasetPersistenceException("Error saving dataset version to Igfs due to: " + e.getMessage(), e);
        }
    }
    
    public String createOrUpdateDatasetVersion(final DatasetSnapshotContext context, InputStream contentStream)
            throws DatasetPersistenceException {
        final String key = getKey(context);
        final String dir = getKeyPrefix(context.getBucketName(),context.getDatasetName());
        LOGGER.debug("Saving dataset version to igfs in bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        IgniteFileSystem fs = fs(context.getBucketName());
        IgfsPath datasetFile = new IgfsPath(key);
        String md5 = null;
        try {
        	IgfsUtils.mkdirs(fs,new IgfsPath(dir));
        	md5 = IgfsUtils.createWithMd5(fs, datasetFile, contentStream);
            LOGGER.debug("Successfully saved dataset version to Igfs bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        } catch (Exception e) {
            throw new DatasetPersistenceException("Error saving dataset version to Igfs due to: " + e.getMessage(), e);
        }
        return md5;
    }
    
    public long appendDatasetVersion(final DatasetSnapshotContext context, InputStream contentStream)
            throws DatasetPersistenceException {
        final String key = getKey(context);
        final String dir = getKeyPrefix(context.getBucketName(),context.getDatasetName());
        LOGGER.debug("Saving dataset version to igfs in bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        IgniteFileSystem fs = fs(context.getBucketName());
        IgfsPath datasetFile = new IgfsPath(key);
        try {        	
        	return IgfsUtils.append(fs, datasetFile, contentStream);            
        } catch (Exception e) {
            throw new DatasetPersistenceException("Error saving dataset version to Igfs due to: " + e.getMessage(), e);
        }
    }

    
    public byte[] getDatasetContent(DatasetSnapshotContext context)
            throws DatasetPersistenceException {
        final String key = getKey(context);
        LOGGER.debug("Retrieving dataset version from Igfs bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        IgfsPath datasetFile = new IgfsPath(key);

        try {
        	IgniteFileSystem fs = fs(context.getBucketName());
            LOGGER.debug("Successfully retrieved dataset version from Igfs bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
            
            return IgfsUtils.read(fs,datasetFile);
            
        } catch (Exception e) {
            throw new DatasetPersistenceException("Error retrieving dataset version from Igfs due to: " + e.getMessage(), e);
        }
    }
    
    public InputStream getDatasetInputStream(DatasetSnapshotContext context)
            throws DatasetPersistenceException {
        final String key = getKey(context);
        LOGGER.debug("Retrieving dataset version from Igfs bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        IgfsPath datasetFile = new IgfsPath(key);

        try {
        	IgniteFileSystem fs = fs(context.getBucketName());        	
        	
            LOGGER.debug("Successfully retrieved dataset version from Igfs bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
            IgfsInputStream in = fs.open(datasetFile);
            
            BufferedInputStream bs = new BufferedInputStream(in,fs.configuration().getBlockSize());
            return bs;
            
        } catch (Exception e) {
            throw new DatasetPersistenceException("Error retrieving dataset version from Igfs due to: " + e.getMessage(), e);
        }
    }
    
    public byte[] getDatasetContent(DatasetSnapshotContext context,long offset,int len)
            throws DatasetPersistenceException {
        final String key = getKey(context);
        LOGGER.debug("Retrieving dataset version from Igfs bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        IgfsPath datasetFile = new IgfsPath(key);

        try {
        	IgniteFileSystem fs = fs(context.getBucketName());        	
        	
            LOGGER.debug("Successfully retrieved dataset version from Igfs bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
            IgfsInputStream in = fs.open(datasetFile);
            in.skip(offset);
            byte[] bs = in.readNBytes(len);            
            return bs;
            
        } catch (Exception e) {
            throw new DatasetPersistenceException("Error retrieving dataset version from Igfs due to: " + e.getMessage(), e);
        }
    }

    
    public void deleteDatasetContent(DatasetSnapshotContext context) throws DatasetPersistenceException {
        final String key = getKey(context);
        LOGGER.debug("Deleting dataset version from Igfs bucket '{}' with key '{}'", new Object[]{s3BucketName, key});

        IgfsPath datasetFile = new IgfsPath(key);

        try {
        	IgniteFileSystem fs = fs(context.getBucketName());
        	IgfsUtils.delete(fs,datasetFile);
            LOGGER.debug("Successfully deleted dataset version from Igfs bucket '{}' with key '{}'", new Object[]{s3BucketName, key});
        } catch (Exception e) {
            throw new DatasetPersistenceException("Error deleting dataset version from Igfs due to: " + e.getMessage(), e);
        }
    }

    
    public void deleteAllDatasetContent(DatasetSnapshotContext context) throws DatasetPersistenceException {        
        final String prefix = getKey(context);
        
        LOGGER.debug("Deleting all dataset versions from Igfs bucket '{}' with prefix '{}'", new Object[]{s3BucketName, prefix});
        IgfsPath datasetFile = new IgfsPath(prefix);
        try {
        	IgniteFileSystem fs = fs(context.getBucketName());
        	IgfsUtils.delete(fs,datasetFile);
            LOGGER.debug("Successfully deleted all dataset versions from Igfs bucket '{}' with prefix '{}'", new Object[]{s3BucketName, prefix});
        } catch (Exception e) {
            throw new DatasetPersistenceException("Error deleting dataset versions from Igfs due to: " + e.getMessage(), e);
        }
    }
    
    public void copy(DatasetSnapshotContext from, DatasetSnapshotContext to) throws DatasetPersistenceException {
        try {
        	final String fromKey = getKey(from);
        	final String toKey = getKey(to);
        	IgniteFileSystem fs = fs(to.getBucketName());
        	IgfsPath fromPath = new IgfsPath(fromKey);
            IgfsPath destPath = new IgfsPath(toKey);        
            
            // todo 使用link技术
            IgfsUtils.copy(fs, fromPath, destPath, StandardCopyOption.REPLACE_EXISTING);
            
        } catch (IOException e) {
        	LOGGER.error("copy:" + e.getMessage(), e);
        	throw new DatasetPersistenceException("copy:" + e.getMessage(),e);            
        }
    }
    
    
    private String getKey(final DatasetSnapshotContext coordinate) {
        final String bundlePrefix = getDatasetPath(coordinate.getBucketName(), coordinate.getDatasetName(), null);        
        return bundlePrefix;
    }
    
    /**
     *  获取Path，不包含文件名
     * @param bucketId
     * @param pathName
     * @return
     */
    private String getKeyPrefix(String bucketId, String pathName) {
    	if(!pathName.endsWith("/")) {
    		int pos = pathName.lastIndexOf('/');
    		if(pos>=0) {
    			pathName = pathName.substring(0,pos);
    		}
    		else {
    			pathName = "";
    		}
    	}
        final String bundlePrefix = getDatasetPath(bucketId, pathName, null);        
        return bundlePrefix;
    }
    
    private String getObjectKey(final IgfsPath path, final String bucket) {
    	if(this.allFS().containsKey(bucket)) {
    		return path.toString().substring(1);
    	}
        if(path.toString().startsWith("/"+bucket)) {
        	return path.toString().substring(bucket.length()+2);
        }
        throw new DatasetPersistenceException("invalide path:" + path.toString());
    }

    private String getDatasetPath(final String bucketId, final String pathName,final String fileName) {
        String sanitizedBucketId = FileUtil.sanitizeFilename(bucketId);
        String sanitizedGroup = FileUtil.sanitizePathname(pathName);
        if(sanitizedBucketId.isEmpty() || this.allFS().containsKey(bucketId)) {
        	if(fileName!=null) {
            	final String santizedFileName = FileUtil.sanitizeFilename(fileName);
            	return "/" + sanitizedGroup + '/' + santizedFileName;
            }
            return "/" + sanitizedGroup;
        }
        if(fileName!=null) {
        	final String santizedFileName = FileUtil.sanitizeFilename(fileName);
        	return "/"+ sanitizedBucketId + "/" + sanitizedGroup + '/' + santizedFileName;
        }
        return "/"+sanitizedBucketId + "/" + sanitizedGroup;
    }    
    
    public ObjectMetadata getObjectMetadata(IgfsFile file,String bucketName) {
    	ObjectMetadata metadata = new ObjectMetadata();
		metadata.setFileName(file.path().name());
		metadata.setContentLength(file.length());
		metadata.setContentType(FileUtil.getContentType(metadata.getFileName()));
		metadata.setContentEncoding(file.property("contentEncoding",null));
		metadata.setLastModified(new Date(file.modificationTime()));
		metadata.setUserMetadata(file.properties());		
		metadata.setETag(file.property("etag",null));
		
		if(metadata.getETag()==null) {
			String eTag = EncryptUtil.encryptByMD5(bucketName+"/"+file.path().name());
			metadata.setETag(eTag+'-'+0);
		}
		
		String expirationTime = file.property("expirationTime",null);
		if(expirationTime!=null) {
			metadata.setExpirationTime(new Date(Long.valueOf(expirationTime)));
		}
		
		return metadata;
    }
    
    public ObjectMetadata getFolderMetadata(IgfsFile file,String bucketName) {
    	ObjectMetadata metadata = new ObjectMetadata();
		metadata.setFileName(file.path().name()+"/");
		metadata.setContentLength(0);
		metadata.setContentType(MediaType.APPLICATION_XML.toString());
		
		metadata.setLastModified(new Date(file.modificationTime()));
		metadata.setUserMetadata(file.properties());
		
		metadata.setETag(file.property("etag",null));
		if(metadata.getETag()==null) {
			String eTag = EncryptUtil.encryptByMD5(bucketName+"/"+metadata.getFileName());
			metadata.setETag(eTag+'-'+0);
		}
		
		String expirationTime = file.property("expirationTime",null);
		if(expirationTime!=null) {
			metadata.setExpirationTime(new Date(Long.valueOf(expirationTime)));
		}
	
		return metadata;
    }
    
    public ObjectMetadata getObjectMetadata(String bucketName, String objectKey) {		
		IgniteFileSystem fs = fs(bucketName);
		String key = getDatasetPath(bucketName,objectKey,null);
		IgfsPath path = new IgfsPath(key);
		IgfsFile file = fs.info(path);
		if(file==null) {
			return null;
		}	
		if(file.isDirectory()) {
			return getFolderMetadata(file,bucketName);
		}
		return getObjectMetadata(file,bucketName);
	}
	
	public List<S3Object> getObjectsAndMetadata(String bucketName, String s3KeyPrefix) {		
		IgniteFileSystem fs = fs(bucketName);
		String key = getDatasetPath(bucketName,s3KeyPrefix==null? "": s3KeyPrefix,null);
		IgfsPath root = new IgfsPath(key);
		List<S3Object>  flows = new ArrayList<>();
		if(!fs.exists(root)) {
			return flows;
		}
		Collection<IgfsFile> datasets = fs.listFiles(root);
		for(IgfsFile dataset: datasets) {
			S3Object flow = new S3Object();
			
			flow.setBucketName(bucketName);
			if(dataset.isFile()) {
				flow.setKey(getObjectKey(dataset.path(),bucketName));
				flow.setMetadata(getObjectMetadata(dataset,bucketName));
			}
			else {
				flow.setKey(getObjectKey(dataset.path(),bucketName)+"/");
				flow.setMetadata(getFolderMetadata(dataset,bucketName));
			}
			flows.add(flow);
		}		
		return flows;
	}
	
	public boolean setObjectMetadata(String bucketName, String s3KeyPrefix,Map<String,String> props) {		
		IgniteFileSystem fs = fs(bucketName);
		String key = getDatasetPath(bucketName,s3KeyPrefix,null);
		IgfsPath path = new IgfsPath(key);
		IgfsFile file = fs.update(path, props);
		if(file==null) {
			return false;
		}
		return true;
		
	}
	
	public Boolean objectIsFolder(String bucketName, String s3KeyPrefix) {		
		IgniteFileSystem fs = fs(bucketName);
		String key = getDatasetPath(bucketName,s3KeyPrefix,null);
		IgfsPath path = new IgfsPath(key);
		IgfsFile file = fs.info(path);
		if(file==null) {
			return null;
		}
		return file.isDirectory();
	}
}
