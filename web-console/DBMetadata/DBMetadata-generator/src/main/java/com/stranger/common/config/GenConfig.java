package com.stranger.common.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

public class GenConfig {
    public  String author;

    public  String packageName;

    public  boolean autoRemovePre;

    public  String tablePrefix;

    public  String fileDownLoadPath;

    public  String urlPrefix;

    @Value("${gen.url-prefix}")
    public void setUrlPrefix(String urlPrefix) {
        urlPrefix = urlPrefix;
    }

    public  String getUrlPrefix() {
        return urlPrefix;
    }


    @Value("${gen.fileDownLoadPath}")
    public void setFileDownLoadPath(String fileDownLoadPath) {
        fileDownLoadPath = fileDownLoadPath;
    }

    public boolean isAutoRemovePre() {
        return autoRemovePre;
    }

    public String getFileDownLoadPath() {
        return fileDownLoadPath;
    }

    public  String getAuthor() {
        return author;
    }

    @Value("${gen.author}")
    public void setAuthor(String author) {
        author = author;
    }

    public  String getPackageName() {
        return packageName;
    }

    @Value("${gen.packageName}")
    public void setPackageName(String packageName) {
        packageName = packageName;
    }

    public  boolean getAutoRemovePre() {
        return autoRemovePre;
    }

    @Value("${gen.autoRemovePre}")
    public void setAutoRemovePre(boolean autoRemovePre) {
        autoRemovePre = autoRemovePre;
    }

    public  String getTablePrefix() {
        return tablePrefix;
    }

    @Value("${gen.tablePrefix}")
    public void setTablePrefix(String tablePrefix) {
        tablePrefix = tablePrefix;
    }
}
