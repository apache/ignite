package org.apache.ignite.ml.tree.randomforest;

public class RandomForestModel extends ModelsComposition<RandomForestTreeModel> implements JSONReadable, JSONWritable {
    /** Serial version uid. */
    private static final long serialVersionUID = 3476345240155508004L;


    public RandomForestModel() {
        super(new ArrayList<>(), new MeanValuePredictionsAggregator());

    }

    public RandomForestModel(List<RandomForestTreeModel> oldModels, PredictionsAggregator predictionsAggregator) {
        super(oldModels, predictionsAggregator);
    }

    /**
     * Returns predictions aggregator.
     */
    @Override
    public PredictionsAggregator getPredictionsAggregator() {
        return predictionsAggregator;
    }

    /**
     * Returns containing models.
     */
    @Override
    public List<RandomForestTreeModel> getModels() {
        return models;
    }

    @Override
    public RandomForestModel fromJSON(Path path) {
            ObjectMapper mapper = new ObjectMapper();

            RandomForestModel mdl;
            try {
                mdl = mapper.readValue(new File(path.toAbsolutePath().toString()), RandomForestModel.class);

                return mdl;
            } catch (IOException e) {
                e.printStackTrace();
            }
        return null;
    }
}