package org.apache.ignite.ml.structures;

import java.io.Serializable;
import org.apache.ignite.ml.math.Vector;

public class Dataset implements Serializable {
    /** Data to keep. */
    protected DatasetRow

        [] data;

    /** Metadata to identify feature. */
    protected FeatureMetadata[] meta;

    /** Amount of instances. */
    private int rowSize;

    /** Amount of attributes in each vector. */
    private int colSize;

    public Dataset(DatasetRow[] data, FeatureMetadata[] meta) {
        this.data = data;
        this.meta = meta;
    }

    /**
     * Creates new Labeled Dataset by given data.
     *
     * @param data Given data. Should be initialized with one vector at least.
     * @param featureNames Column names.
     * @param colSize Amount of observed attributes in each vector.
     */
    public Dataset(LabeledVector[] data, String[] featureNames, int colSize) {
        this(data.length, colSize, featureNames);
        assert data != null;
        this.data = data;
    }

    /**
     * Creates new Labeled Dataset and initialized with empty data structure.
     *
     * @param rowSize Amount of instances. Should be > 0.
     * @param colSize Amount of attributes. Should be > 0
     * @param featureNames Column names.
     */
    public Dataset(int rowSize, int colSize, String[] featureNames){
        assert rowSize > 0;
        assert colSize > 0;

        if(featureNames == null) generateFeatureNames();
        else {
            assert colSize == featureNames.length;
            convertStringNamesToFeatureMetadata(featureNames);
        }

        this.rowSize = rowSize;
        this.colSize = colSize;
    }



    private void convertStringNamesToFeatureMetadata(String[] featureNames) {
        this.meta = new FeatureMetadata[featureNames.length];
        for (int i = 0; i < featureNames.length; i++)
            this.meta[i] = new FeatureMetadata(featureNames[i]);
    }

    /** */
    private void generateFeatureNames() {
        String[] featureNames = new String[colSize];

        for (int i = 0; i < colSize; i++)
            featureNames[i] = "f_" + i;

        convertStringNamesToFeatureMetadata(featureNames);
    }

    /**
     * Returns feature name for column with given index.
     *
     * @param i The given index.
     * @return Feature name.
     */
    public String getFeatureName(int i){
        return meta[i].name();
    }

    public DatasetRow[] data() {
        return data;
    }

    public void setData(DatasetRow[] data) {
        this.data = data;
    }

    public FeatureMetadata[] meta() {
        return meta;
    }

    public void setMeta(FeatureMetadata[] meta) {
        this.meta = meta;
    }

    /**
     * Gets amount of attributes.
     *
     * @return Amount of attributes in each Labeled Vector.
     */
    public int colSize(){
        return colSize;
    }

    /**
     * Gets amount of observation.
     *
     * @return Amount of rows in dataset.
     */
    public int rowSize(){
        return rowSize;
    }

    /**
     * Retrieves Labeled Vector by given index.
     *
     * @param idx Index of observation.
     * @return Labeled features.
     */
    public DatasetRow getRow(int idx){
        return data[idx];
    }

    /**
     * Get the features.
     *
     * @param idx Index of observation.
     * @return Vector with features.
     */
    public Vector features(int idx){
        assert idx < rowSize;
        assert data != null;
        assert data[idx] != null;

        return data[idx].features();
    }

}
