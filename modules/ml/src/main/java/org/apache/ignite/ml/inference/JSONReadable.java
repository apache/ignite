package org.apache.ignite.ml.inference;

public interface JSONReadable {
    IgniteModel<Vector, ? extends Number> fromJSON(Path path);
}