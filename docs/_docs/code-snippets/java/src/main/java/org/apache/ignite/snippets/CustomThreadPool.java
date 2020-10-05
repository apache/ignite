package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

public class CustomThreadPool {

    void customPool() {

        // tag::pool-config[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setExecutorConfiguration(new ExecutorConfiguration("myPool").setSize(16));
        // end::pool-config[]

        Ignite ignite = Ignition.start(cfg);

        ignite.compute().run(new OuterRunnable());

    }

    // tag::inner-runnable[]
    public class InnerRunnable implements IgniteRunnable {
        @Override
        public void run() {
            System.out.println("Hello from inner runnable!");
        }
    }
    // end::inner-runnable[]

    // tag::outer-runnable[]
    public class OuterRunnable implements IgniteRunnable {
        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public void run() {
            // Synchronously execute InnerRunnable in a custom executor.
            ignite.compute().withExecutor("myPool").run(new InnerRunnable());
            System.out.println("outer runnable is executed");
        }
    }
    // end::outer-runnable[]

    public static void main(String[] args) {
        CustomThreadPool ctp = new CustomThreadPool();
        ctp.customPool();
    }
}
