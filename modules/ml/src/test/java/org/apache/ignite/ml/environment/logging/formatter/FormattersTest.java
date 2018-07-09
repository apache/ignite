package org.apache.ignite.ml.environment.logging.formatter;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FormattersTest {
    public static class Model1 implements Model<Vector, Double> {
        public final static String name = "Model1";

        @Override public Double apply(Vector vector) {
            return vector.get(0);
        }
    }

    public static class Model2 implements Model<Vector, Double> {
        public final static String name = "Model2";

        @Override public Double apply(Vector vector) {
            return vector.get(1);
        }
    }

    public static class Model3 implements Model<Vector, Double> {
        @Override public Double apply(Vector vector) {
            return 0.0;
        }
    }

    static {
        Formatters.getInstance().registerFormatter(Model1.class, model -> model.name);
        Formatters.getInstance().registerFormatter(Model2.class, model -> model.name);
    }

    @Test
    public void test1() {
        assertEquals(Model1.name, Formatters.getInstance().format(new Model1()));
        assertEquals(Model2.name, Formatters.getInstance().format(new Model2()));
    }

    @Test
    public void test2() {
        assertEquals(Model3.class.getName(), Formatters.getInstance().format(new Model3()));
    }
}
