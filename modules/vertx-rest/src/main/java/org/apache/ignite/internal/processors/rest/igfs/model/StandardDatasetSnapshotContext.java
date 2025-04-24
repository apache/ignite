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
package org.apache.ignite.internal.processors.rest.igfs.model;


import java.util.Objects;

/**
 * Standard implementation of DatasetSnapshotContext.
 */
public class StandardDatasetSnapshotContext implements DatasetSnapshotContext {

    private final String bucketId;
    private final String bucketName;
    private final String datasetId;
    private final String datasetName;    
    private final int version;
    private final String comments;
    private final String fileName;
    private final String author;
    private final long snapshotTimestamp;

    private StandardDatasetSnapshotContext(final Builder builder) {
        this.bucketId = builder.bucketId;
        this.bucketName = builder.bucketName;
        this.datasetId = builder.datasetId;
        this.datasetName = builder.datasetName;       
        this.version = builder.version;
        this.comments = builder.comments;
        this.fileName = builder.fileName;
        this.author = builder.author;
        this.snapshotTimestamp = builder.snapshotTimestamp; 
    }

    @Override
    public String getBucketId() {
        return bucketId;
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public String getDatasetId() {
        return datasetId;
    }

    @Override
    public String getDatasetName() {
        return datasetName;
    }
   
    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public String getComments() {
        return comments;
    }
    
	@Override
    public String getFileName() {
		return fileName;
	}

    @Override
    public long getSnapshotTimestamp() {
        return snapshotTimestamp;
    }

    @Override
    public String getAuthor() {
        return author;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StandardDatasetSnapshotContext that = (StandardDatasetSnapshotContext) o;
        return version == that.version && snapshotTimestamp == that.snapshotTimestamp
                && Objects.equals(bucketId, that.bucketId)
                && Objects.equals(bucketName, that.bucketName)
                && Objects.equals(datasetId, that.datasetId)
                && Objects.equals(datasetName, that.datasetName)
                && Objects.equals(comments, that.comments)
                && Objects.equals(author, that.author);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, bucketName, datasetId, datasetName, version, comments, author, snapshotTimestamp);
    }

	/**
     * Builder for creating instances of StandardDatasetSnapshotContext.
     */
    public static class Builder {

        private String bucketId;
        private String bucketName;
        private String datasetId;
        private String datasetName;
        
        private int version;
        private String comments;
        private String fileName;
        private String author;
        private long snapshotTimestamp;

        public Builder() {

        }
        
        public Builder(final Bucket bucket) {
        	bucketId(bucket.getName());
            bucketName(bucket.getName());
            author(bucket.getAuthor());
        }
        
        public Builder(final Bucket bucket, final S3Object object) {
            bucketId(bucket.getName());
            bucketName(bucket.getName());
            author(bucket.getAuthor());
            datasetId(object.getKey());
            datasetName(object.getKey());            
            fileName(object.getMetadata().getFileName());
            snapshotTimestamp(object.getMetadata().getLastModified().getTime());
        }
       

        public Builder bucketId(final String bucketId) {
            this.bucketId = bucketId;
            return this;
        }

        public Builder bucketName(final String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder datasetId(final String datasetId) {
            this.datasetId = datasetId;
            return this;
        }

        public Builder datasetName(final String datasetName) {
            this.datasetName = datasetName;
            return this;
        }
        
        public Builder version(final int version) {
            this.version = version;
            return this;
        }

        public Builder comments(final String comments) {
            this.comments = comments;
            return this;
        }
        
        public Builder fileName(final String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder author(final String author) {
            this.author = author;
            return this;
        }

        public Builder snapshotTimestamp(final long snapshotTimestamp) {
            this.snapshotTimestamp = snapshotTimestamp;
            return this;
        }

        public StandardDatasetSnapshotContext build() {
            return new StandardDatasetSnapshotContext(this);
        }

    }

}
