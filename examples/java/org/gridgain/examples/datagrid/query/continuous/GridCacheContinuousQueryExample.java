// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.query.continuous;

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * This examples demonstrates continuous query API.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}.
 * <h2 class="header">NOTE</h2>
 * Under some concurrent circumstances callback may get several notifications
 * for one cache update.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheContinuousQueryExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example on the grid.
     *
     * @param args Command line arguments. None required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void main(String[] args) throws GridException, InterruptedException {
        try (Grid g = GridGain.start(args.length == 0 ? "examples/config/example-cache.xml" : args[0])) {
            GridCacheContinuousQuery <UUID, Person> qry = null;

            try {
                GridCache<UUID, Person> cache = g.cache(CACHE_NAME);

                // Create several persons and put them to cache before query execution.
                Person p1 = new Person("John", "Doe", 500);
                Person p2 = new Person("Jane", "Doe", 2000);
                Person p3 = new Person("John", "Smith", 1000);

                cache.putx(p1.getId(), p1);
                cache.putx(p2.getId(), p2);
                cache.putx(p3.getId(), p3);

                // Create new continuous query.
                qry = cache.queries().createContinuousQuery();

                // Callback that is called locally when update notifications are received.
                // It simply prints out information about all created persons.
                qry.callback(new GridBiPredicate<UUID, Collection<Map.Entry<UUID, Person>>>() {
                    @Override public boolean apply(UUID uuid, Collection<Map.Entry<UUID, Person>> entries) {
                        for (Map.Entry<UUID, Person> e : entries) {
                            Person p = e.getValue();

                            System.out.println(">>>");
                            System.out.println(">>> " + p.getFirstName() + " " + p.getLastName() +
                                "'s salary is " + p.getSalary());
                            System.out.println(">>>");
                        }

                        return true;
                    }
                });

                // This query will return persons with salary above 1000.
                qry.filter(new GridBiPredicate<UUID, Person>() {
                    @Override public boolean apply(UUID uuid, Person person) {
                        return person.getSalary() > 1000;
                    }
                });

                // Execute query.
                qry.execute();

                // Create and put to cache some persons. This is done after query execution,
                // but you you will still receive callbacks.
                Person p4 = new Person("Mike", "Smith", 2000);
                Person p5 = new Person("Jane", "White", 500);
                Person p6 = new Person("Mike", "Green", 1500);

                cache.putx(p4.getId(), p4);
                cache.putx(p5.getId(), p5);
                cache.putx(p6.getId(), p6);

                // Wait for a while while callback is notified about remaining puts.
                Thread.sleep(2000);
            }
            finally {
                // Cancel query.
                if (qry != null)
                    qry.cancel();
            }
        }
    }
}
