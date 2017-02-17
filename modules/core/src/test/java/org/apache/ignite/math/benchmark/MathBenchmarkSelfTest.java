package org.apache.ignite.math.benchmark;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** */
public class MathBenchmarkSelfTest {
    /** */ @Test
    public void demoTest() throws Exception {
        for (int i = 0; i < 2; i++)
            new MathBenchmark("demo test")
                .outputFileName(null) // IMPL NOTE this is to write output into console instead of a file
                .tag(null) // IMPL NOTE try null for tag, expect it to be formatted reasonably
                .comments(null) // IMPL NOTE try null for comments, expect it to be formatted reasonably
                .execute(() -> {
                    double seed = 1.1;

                    for (int cnt = 0; cnt < 1000; cnt++) {
                        seed = Math.pow(seed, 2);

                        assertTrue(seed > 0);
                    }
                });
    }

    /** */ @Test
    public void configTest() throws Exception {
        new MathBenchmark("demo config test")
            .outputFileName(null)
            .measurementTimes(2)
            .warmupTimes(0)
            .tag("demo tag")
            .comments("demo comments")
            .execute(() -> System.out.println("config test"));
    }

    /** */ @Test(expected = IllegalArgumentException.class)
    public void emptyNameTest() throws Exception {
        new MathBenchmark("")
            .outputFileName("whatever")
            .measurementTimes(1)
            .warmupTimes(1)
            .tag("empty name test tag")
            .comments("empty name test comments")
            .execute(() -> System.out.println("empty name test"));
    }
}
