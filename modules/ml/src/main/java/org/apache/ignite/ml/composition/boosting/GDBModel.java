package org.apache.ignite.ml.composition.boosting;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.WeightedPredictionsAggregator;
import org.apache.ignite.ml.inference.json.JSONModel;
import org.apache.ignite.ml.inference.json.JSONModelMixIn;
import org.apache.ignite.ml.inference.json.JSONWritable;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.tree.DecisionTreeModel;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import static org.apache.ignite.ml.inference.json.JacksonHelper.readAndValidateBasicJsonModelProperties;

/**
 * GDB model.
 */
public final class GDBModel extends ModelsComposition<DecisionTreeModel> implements JSONWritable {
    /** Serial version uid. */
    private static final long serialVersionUID = 3476661240155508004L;

    /** Internal to external lbl mapping. */
    @JsonIgnore private IgniteFunction<Double, Double> internalToExternalLblMapping;

    /**
     * Creates an instance of GDBModel.
     *
     * @param models Models.
     * @param predictionsAggregator Predictions aggregator.
     * @param internalToExternalLblMapping Internal to external lbl mapping.
     */
    public GDBModel(List<? extends IgniteModel<Vector, Double>> models,
                    WeightedPredictionsAggregator predictionsAggregator,
                    IgniteFunction<Double, Double> internalToExternalLblMapping) {

        super((List<DecisionTreeModel>) models, predictionsAggregator);
        this.internalToExternalLblMapping = internalToExternalLblMapping;
    }

    private GDBModel() {
    }

    public GDBModel withLblMapping(IgniteFunction<Double, Double> internalToExternalLblMapping) {
        this.internalToExternalLblMapping = internalToExternalLblMapping;
        return this;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector features) {
        if(internalToExternalLblMapping == null) {
            throw new IllegalArgumentException("The mapping should not be empty. Initialize it with apropriate function. ");
        } else {
            return internalToExternalLblMapping.apply(super.predict(features));
        }
    }

    /** {@inheritDoc} */
    @Override public void toJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.addMixIn(GDBModel.class, JSONModelMixIn.class);

        ObjectWriter writer = mapper
            .writerFor(GDBModel.class)
            .withAttribute("formatVersion", JSONModel.JSON_MODEL_FORMAT_VERSION)
            .withAttribute("timestamp", System.currentTimeMillis())
            .withAttribute("uid", "dt_" + UUID.randomUUID().toString())
            .withAttribute("modelClass", GDBModel.class.getSimpleName());

        try {
            File file = new File(path.toAbsolutePath().toString());
            writer.writeValue(file, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Loads RandomForestModel from JSON file. */
    public static GDBModel fromJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        GDBModel mdl;
        try {
            readAndValidateBasicJsonModelProperties(path, mapper, GDBModel.class.getSimpleName());
            mdl = mapper.readValue(new File(path.toAbsolutePath().toString()), GDBModel.class);
            return mdl;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
