package org.apache.ignite.internal.processors.rest.igfs.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class ObjectMetadata {
    private String contentType;
    private long contentLength;
    private Date lastModified;
    private Date expirationTime;
    private String cacheControl;
    private String contentEncoding;
    private String fileName;
    private String ETag;
    private Map<String,String> userMetadata;
    
    public void setUserMetadata(Map<String,String> userMetadata) {
    	this.userMetadata = userMetadata;
    }
    
    public void addUserMetadata(String key, String value) {
    	if(userMetadata==null) userMetadata = new HashMap<>();
    	userMetadata.put(key,value);
    }
    
    public Map<String,String> getUserMetadata(){
    	return userMetadata;
    }
    
    //Gets a map of the raw metadata/headers for the associated object.
    public Object getUserMetadataValue(String key) {
    	if(userMetadata==null) return null;
    	return userMetadata.get(key);
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public long getContentLength() {
        return contentLength;
    }

    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

	public Date getExpirationTime() {
		return expirationTime;
	}

	public void setExpirationTime(Date expirationTime) {
		this.expirationTime = expirationTime;
	}

	public String getCacheControl() {
		return cacheControl;
	}

	public void setCacheControl(String cacheControl) {
		this.cacheControl = cacheControl;
	}

	public String getContentEncoding() {
		return contentEncoding;
	}

	public void setContentEncoding(String contentEncoding) {
		this.contentEncoding = contentEncoding;
	}

	public String getETag() {
		return ETag;
	}

	public void setETag(String eTag) {
		ETag = eTag;
	}
    
}
