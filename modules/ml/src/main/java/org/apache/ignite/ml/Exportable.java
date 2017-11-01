package org.apache.ignite.ml;

/**
 * Interface for exportable models({@link Model}).
 *
 * @see Exporter
 */
public interface Exportable<D> {
    /**
     * Save model by the given path.
     *
     * @param exporter Exporter.
     * @param path Path to saved model.
     */
    public <P> void saveModel(Exporter<D, P> exporter, P path);
}
