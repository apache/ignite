package org.apache.ignite.internal.processors.rest.igfs.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource("classpath:application.properties")
public class SystemConfig {
    
	private String tempPath = "/tmp";
    private String dataPath = "file-uploads/";
    
    @Value("${igfs.s3BucketName:igfs}")
    private String s3BucketName = "igfs";
    
    @Value("${igfs.endpointOverride:}")
    private String endpointOverride = null;
    
    @Value("${igfs.accessKey:root}")
    private String accessKey = "root";
    
    @Value("${igfs.secretAccessKey:123456}")
    private String secretAccessKey = "123456";
        

    public String getEndpointOverride() {
    	String endpoint = endpointOverride;    	
		return endpoint;
	}

	public void setEndpointOverride(String endpointOverride) {
		this.endpointOverride = endpointOverride;
	}

	public String getTempPath() {
        return tempPath;
    }

    public void setTempPath(String tempPath) {
        this.tempPath = tempPath;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getAccessKey() {
        return accessKey ;
    }

    public void setAccessKey(String username) {
        this.accessKey = username;
    }

    public String getSecretAccessKey() {
        return secretAccessKey ;
    }

    public void setSecretAccessKey(String password) {
        this.secretAccessKey = password;
    }    
   
	public String getS3BucketName() {
		return s3BucketName;
	}

	public void setS3BucketName(String s3BucketName) {
		this.s3BucketName = s3BucketName;
	}

}
