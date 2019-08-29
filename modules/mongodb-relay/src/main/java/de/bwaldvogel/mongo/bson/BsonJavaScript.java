package de.bwaldvogel.mongo.bson;

import java.util.Objects;

public final class BsonJavaScript implements Bson {

    private static final long serialVersionUID = 1L;

    private final String code;

    public BsonJavaScript(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BsonJavaScript that = (BsonJavaScript) o;
        return Objects.equals(getCode(), that.getCode());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCode());
    }


}
