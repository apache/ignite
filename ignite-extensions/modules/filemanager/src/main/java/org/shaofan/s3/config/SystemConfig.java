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
    
    //  单个数据大小
    int maxFileSize = 2*1024*1024*1024; // 2G
    
	/// 总上传数据大小
    int maxRequestSize = 64*1024*1024; // 64M
    
    int fileSizeThreshold = 64*1024*1024*1024; // 64G
    

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
    
    public int getMaxFileSize() {
		return maxFileSize;
	}

	public void setMaxFileSize(int maxFileSize) {
		this.maxFileSize = maxFileSize;
	}

	public int getMaxRequestSize() {
		return maxRequestSize;
	}

	public void setMaxRequestSize(int maxRequestSize) {
		this.maxRequestSize = maxRequestSize;
	}

	public int getFileSizeThreshold() {
		return fileSizeThreshold;
	}

	public void setFileSizeThreshold(int fileSizeThreshold) {
		this.fileSizeThreshold = fileSizeThreshold;
	}

	public String getS3BucketName() {
		return s3BucketName;
	}

	public void setS3BucketName(String s3BucketName) {
		this.s3BucketName = s3BucketName;
	}

}
