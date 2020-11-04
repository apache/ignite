package org.apache.ignite.ml.inference;

public class JSONModel {
    public String applicationName = "Apache Ignite";

    public String versionName;

    @Override
    public String toString() {
        return "JSONModel{" +
                "applicationName='" + applicationName + '\'' +
                ", versionName='" + versionName + '\'' +
                '}';
    }
}
