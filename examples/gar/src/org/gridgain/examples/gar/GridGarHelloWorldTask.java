/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.gar;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.springframework.beans.factory.support.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.core.io.*;

import java.io.*;
import java.util.*;

/**
 * This class defines grid task for this example. Grid task is responsible for
 * splitting the task into jobs. This particular implementation splits given
 * string into individual words and creates grid jobs for each word.
 * Task class in that example should be placed in GAR file.
 * See {@code GridGarHelloWorldExample} for more details.
 */
@GridComputeTaskName("GridGarHelloWorldTask")
public class GridGarHelloWorldTask  extends GridComputeTaskSplitAdapter<String, Object> {
    /** {@inheritDoc} */
    @Override public Collection<? extends GridComputeJob> split(int gridSize, String arg) throws GridException {
        // Create Spring context.
        AbstractBeanFactory fac = new XmlBeanFactory(
            new ClassPathResource("org/gridgain/examples/gar/gar-spring-bean.xml", getClass().getClassLoader()));

        fac.setBeanClassLoader(getClass().getClassLoader());

        // Load imported bean from GAR/lib folder.
        GridGarHelloWorldBean bean = (GridGarHelloWorldBean)fac.getBean("example.bean");

        String msg = bean.getMessage(arg);

        assert msg != null;

        // Split the passed in phrase into multiple words separated by spaces.
        List<String> words = Arrays.asList(msg.split(" "));

        Collection<GridComputeJob> jobs = new ArrayList<>(words.size());

        // Use imperative OOP APIs.
        for (String word : words) {
            // Every job gets its own word as an argument.
            jobs.add(new GridComputeJobAdapter(word) {
                /*
                 * Simply prints the job's argument.
                 */
                @Override public Serializable execute() {
                    System.out.println(">>>");
                    System.out.println(">>> Printing '" + argument(0) + "' on this node from grid job.");
                    System.out.println(">>>");

                    // This job does not return any result.
                    return null;
                }
            });
        }

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }
}
