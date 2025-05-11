package org.apache.ignite.internal.processors.rest.igfs.model;

public class PartETag {
    private int partNumber;
    private String eTag;

    public PartETag(int partNumber, String eTag) {
        this.partNumber = partNumber;
        this.eTag = eTag;
    }

    public int getPartNumber() {
        return partNumber;
    }

    public String geteTag() {
        return eTag;
    }
}
