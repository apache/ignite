package de.kp.works.ignite.gremlin.sql;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A {@link TraversalSource} implementation that spawns {@link SparqlTraversal} instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IgniteGraphTraversalSource extends GraphTraversalSource {

    public IgniteGraphTraversalSource(final Graph graph, final TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }

    public IgniteGraphTraversalSource(final Graph graph) {
        super(graph);
    }

    public IgniteGraphTraversalSource(final RemoteConnection connection) {
        super(connection);
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public IgniteGraphTraversalSource clone() {
        return (IgniteGraphTraversalSource) super.clone();
    }

    //// CONFIGURATIONS


    @Override
    public IgniteGraphTraversalSource with(final String key) {
        return (IgniteGraphTraversalSource) super.with(key);
    }

    @Override
    public IgniteGraphTraversalSource with(final String key, final Object value) {
        return (IgniteGraphTraversalSource) super.with(key, value);
    }

    @Override
    public IgniteGraphTraversalSource withComputer(final Computer computer) {
        return (IgniteGraphTraversalSource) super.withComputer(computer);
    }

    @Override
    public IgniteGraphTraversalSource withComputer(final Class<? extends GraphComputer> graphComputerClass) {
        return (IgniteGraphTraversalSource) super.withComputer(graphComputerClass);
    }

    @Override
    public IgniteGraphTraversalSource withComputer() {
        return (IgniteGraphTraversalSource) super.withComputer();
    }

    @Override
    public <A> IgniteGraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue, final BinaryOperator<A> reducer) {
        return (IgniteGraphTraversalSource) super.withSideEffect(key, initialValue, reducer);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSideEffect(final String key, final A initialValue, final BinaryOperator<A> reducer) {
        return (IgniteGraphTraversalSource) super.withSideEffect(key, initialValue, reducer);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSideEffect(final String key, final A initialValue) {
        return (IgniteGraphTraversalSource) super.withSideEffect(key, initialValue);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue) {
        return (IgniteGraphTraversalSource) super.withSideEffect(key, initialValue);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (IgniteGraphTraversalSource) super.withSack(initialValue, splitOperator, mergeOperator);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (IgniteGraphTraversalSource) super.withSack(initialValue, splitOperator, mergeOperator);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSack(final A initialValue) {
        return (IgniteGraphTraversalSource) super.withSack(initialValue);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSack(final Supplier<A> initialValue) {
        return (IgniteGraphTraversalSource) super.withSack(initialValue);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        return (IgniteGraphTraversalSource) super.withSack(initialValue, splitOperator);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        return (IgniteGraphTraversalSource) super.withSack(initialValue, splitOperator);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
        return (IgniteGraphTraversalSource) super.withSack(initialValue, mergeOperator);
    }

    @Override
    public <A> IgniteGraphTraversalSource withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
        return (IgniteGraphTraversalSource) super.withSack(initialValue, mergeOperator);
    }

    @Override
    public IgniteGraphTraversalSource withBulk(final boolean useBulk) {
        return (IgniteGraphTraversalSource) super.withBulk(useBulk);
    }

    @Override
    public IgniteGraphTraversalSource withPath() {
        return (IgniteGraphTraversalSource) super.withPath();
    }

    @Override
    public IgniteGraphTraversalSource withStrategies(final TraversalStrategy... traversalStrategies) {
        return (IgniteGraphTraversalSource) super.withStrategies(traversalStrategies);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public IgniteGraphTraversalSource withoutStrategies(final Class<? extends TraversalStrategy>... traversalStrategyClasses) {
        return (IgniteGraphTraversalSource) super.withoutStrategies(traversalStrategyClasses);
    }

    /**
     * The start step for a SQL based traversal that accepts a string representation of the query to execute.
     */
    public <S> GraphTraversal<S,?> selectQuery(final String table,final String sql, Object[] args) {
        final IgniteGraphTraversalSource clone = this.withStrategies(IgniteSqlStrategy.instance()).clone();

        // the inject() holds the sparql which the SparqlStrategy then detects and converts to a traversal
        clone.bytecode.addStep(GraphTraversal.Symbols.inject, "select", table, sql, args);
        final GraphTraversal.Admin<S, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        
        return traversal.addStep(new InjectStep<>(traversal,"select", table, sql, args));
    }
    
    /**
     * The start step for a SQL based traversal that accepts a string representation of the query to execute.
     */
    public <S> GraphTraversal<S,?> selectQuery(final String table, Object[] args) {
        return selectQuery(table, null, args);
    }
    
    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the specified label. If the {@code label} is
     * {@code null} then it will default to {@link Vertex#DEFAULT_LABEL}.
     */
    public GraphTraversal<Vertex, Vertex> addDoc(final String label) {
        final IgniteGraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.addV, label);
        clone.bytecode.addStep(GraphTraversal.Symbols.property, "_class", "document");
        
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        AddVertexStartStep step = new AddVertexStartStep(traversal, label);
        step.configure("_class","document");
        AddPropertyStep step2 = new AddPropertyStep<>(traversal, VertexProperty.Cardinality.single, "_class", "document");
        
        return traversal.addStep(step).addStep(step2);
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the label as determined by a {@link Traversal}. If the
     * {@code vertexLabelTraversal} is {@code null} then it will default to {@link Vertex#DEFAULT_LABEL}.
     */
    public GraphTraversal<Vertex, Vertex> addDoc(final Traversal<?, String> vertexLabelTraversal) {
        final IgniteGraphTraversalSource clone = this.clone();       
        clone.bytecode.addStep(GraphTraversal.Symbols.addV, vertexLabelTraversal);
        clone.bytecode.addStep(GraphTraversal.Symbols.property, "_class", "document");
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        AddVertexStartStep step = new AddVertexStartStep(traversal, vertexLabelTraversal);
        step.configure("_class","document");
        AddPropertyStep step2 = new AddPropertyStep<>(traversal, VertexProperty.Cardinality.single, "_class", "document");
        
        return traversal.addStep(step).addStep(step2);
    }
    
    
    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the specified label. If the {@code label} is
     * {@code null} then it will default to {@link Vertex#DEFAULT_LABEL}.
     */
    public <S> GraphTraversal<S, ?> addIndex(final String label,final String propertyKey,Boolean isUnique) {
    	final IgniteGraphTraversalSource clone = this.withStrategies(IgniteSqlStrategy.instance()).clone();

        // the inject() holds the sparql which the SparqlStrategy then detects and converts to a traversal
        clone.bytecode.addStep(GraphTraversal.Symbols.inject, "addIndex", label, propertyKey, isUnique);
        final GraphTraversal.Admin<S, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        
        return traversal.addStep(new InjectStep<>(traversal, "addIndex", label, propertyKey, isUnique));
    }
}