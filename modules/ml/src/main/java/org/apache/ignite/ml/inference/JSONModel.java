package org.apache.ignite.ml.inference;

import org.apache.ignite.ml.IgniteModel;

public abstract class JSONModel {
    protected String igniteVersion = "2.10.0";
    protected Long timestamp;
    protected String uid;
    protected String modelClass;

    public abstract IgniteModel convert();

    protected JSONModel(Long timestamp, String uid, String modelClass) {
        this.timestamp = timestamp;
        this.uid = uid;
        this.modelClass = modelClass;
    }
}
