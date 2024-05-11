package org.shaofan.s3.config;

import org.springframework.stereotype.Component;

@Component
public class SystemConfig {
    private String tempPath = "/tmp";
    private String dataPath = "data/";
    private String s3BucketName = "igfs";
    private String endpointOverride = null;
    private String accessKey = "root";
    private String secretAccessKey = "123456";
    
        

    public String getEndpointOverride() {
		return endpointOverride;
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
