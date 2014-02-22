// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.misc.resources;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;

import java.io.*;
import java.util.*;

/**
 * This class defines grid task for this example.
 * Grid task is responsible for splitting the task into jobs. This particular
 * implementation splits given string into individual words and creates
 * grid jobs for each word. Every job will send data through context injected in
 * job as user resource.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridResourcesTask extends GridComputeTaskSplitAdapter<String, Integer> {
    /**
     * Splits the passed in phrase into words and creates a job for every
     * word. Every job will print out the word and return number of letters in that
     * word. Job use context to store data through injected context.
     *
     * @param gridSize Number of nodes in the grid.
     * @param phrase Any phrase.
     * @return Created grid jobs for remote execution.
     * @throws GridException If split failed.
     */
    @Override public Collection<? extends GridComputeJob> split(int gridSize, String phrase) throws GridException {
        // Split the passed in phrase into multiple words separated by spaces.
        String[] words = phrase.split(" ");

        Collection<GridComputeJob> jobs = new ArrayList<>(words.length);

        for (String word : words) {
            // Every job gets its own word as an argument.
            jobs.add(new GridComputeJobAdapter(word) {
                @GridUserResource
                private transient GridResourcesContext ctx;

                /*
                 * Simply prints the word passed into the job and
                 * returns number of letters in that word.
                 */
                @Override public Serializable execute() {
                    String word = argument(0);

                    assert word != null;

                    System.out.println(">>>");
                    System.out.println(">>> Printing '" + word + "' on this node from grid job.");
                    System.out.println(">>>");

                    try {
                        ctx.sendData(word);
                    }
                    catch (Exception e) {
                        e.printStackTrace(System.err);
                    }

                    // Return number of letters in the word.
                    return word.length();
                }
            });
        }

        return jobs;
    }

    /**
     * Sums up all characters returns from all jobs and returns a
     * total number of characters in the phrase.
     *
     * @param results Job results.
     * @return Number of characters for the phrase passed into
     *      {@code split(gridSize, phrase)} method above.
     * @throws GridException If reduce failed.
     */
    @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
        int sum = 0;

        for (GridComputeJobResult res : results)
            sum += res.<Integer>getData();

        return results.size() - 1 + sum;
    }
}
