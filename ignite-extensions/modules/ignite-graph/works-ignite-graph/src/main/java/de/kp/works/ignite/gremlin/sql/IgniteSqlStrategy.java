package de.kp.works.ignite.gremlin.sql;

import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * This {@link TraversalStrategy} is used in conjunction with the {@link SparqlTraversalSource} which has a single
 * {@code sparql()} start step. That step adds a {@link InjectStep} to the traversal with the SPARQL query within
 * it as a string value. This strategy finds that step and compiles it to a Gremlin traversal which then replaces
 * the {@link InjectStep}.
 *
 * For remote bytecode execution, note that the {@code sparql-gremlin} dependencies need to be on the server. The
 * way remoting works - see {@code RemoteStep} - prevents modified bytecode from being submitted to the server, so
 * technically, this strategy can't be applied first on the client.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IgniteSqlStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {
    private static final IgniteSqlStrategy INSTANCE = new IgniteSqlStrategy();

    private static final Set<Class<? extends DecorationStrategy>> PRIORS = Collections.singleton(RemoteStrategy.class);

    private IgniteSqlStrategy() {}

    public static IgniteSqlStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends DecorationStrategy>> applyPrior() {
        return PRIORS;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.isRoot()))
            return;

        // assumes that the traversal starts with the single inject step that holds the sparql query
        if (traversal.getStartStep() instanceof InjectStep) {
            final InjectStep stepWithSparql = (InjectStep) traversal.getStartStep();
            final Object[] injections = stepWithSparql.getInjections();

            // further assumes that there is just one argument to that injection which is a string (i.e. sparql query)
            if (injections.length > 1 && injections[0] instanceof String && injections[0].equals("select")) {
                final String table = (String) injections[1];
                final String sparql = (String) injections[2];
                Object[] args = Arrays.copyOfRange(injections, 3, injections.length);
                SqlToGremlinCompiler cc = new SqlToGremlinCompiler(traversal);

                // try to grab the TraversalSource from the Traversal, but if it's not there then try to the Graph
                // instance and spawn one off from there.
                GraphTraversalSource g = (GraphTraversalSource) traversal.getTraversalSource().orElseGet(() -> traversal.getGraph().map(Graph::traversal).get());
                final Traversal<Vertex, ?> sparqlTraversal = cc.select(
                        g, table, sparql, args);
                TraversalHelper.insertTraversal(stepWithSparql, sparqlTraversal.asAdmin(), traversal);
                traversal.removeStep(stepWithSparql);
            }
            
            else if (injections.length > 1 && injections[0] instanceof String && injections[0].equals("addIndex")) {
                final String label = (String) injections[1];
                final String propertyKey = (String) injections[2];
                final Boolean isUnique = (Boolean) injections[3];
                SqlToGremlinCompiler cc = new SqlToGremlinCompiler(traversal);

                // try to grab the TraversalSource from the Traversal, but if it's not there then try to the Graph
                // instance and spawn one off from there.
                GraphTraversalSource g = (GraphTraversalSource) traversal.getTraversalSource().orElseGet(() -> traversal.getGraph().map(Graph::traversal).get());
                final Traversal<Vertex, ?> sparqlTraversal = cc.addIndex(
                        g, label, propertyKey, isUnique);
                TraversalHelper.insertTraversal(stepWithSparql, sparqlTraversal.asAdmin(), traversal);
                traversal.removeStep(stepWithSparql);
            }
        }
    }
} 