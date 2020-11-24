package org.apache.ignite.ml.composition.boosting;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.WeightedPredictionsAggregator;
import org.apache.ignite.ml.inference.JSONWritable;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.tree.DecisionTreeModel;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

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

    public static GDBModel fromJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper();

        GDBModel mdl;
        try {
            mdl = mapper.readValue(new File(path.toAbsolutePath().toString()), GDBModel.class);

            return mdl;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
