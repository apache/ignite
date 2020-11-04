package org.apache.ignite.ml.inference;

public interface JSONWritable {
    default void toJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            File file = new File(path.toAbsolutePath().toString());
            mapper.writeValue(file, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}