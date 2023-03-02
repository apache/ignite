package de.kp.works.ignite.gremlin.process.step.sideEffect;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.gremlin.CloseableIteratorUtils;
import de.kp.works.ignite.gremlin.IgniteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public final class IgniteGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public IgniteGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        if (null == this.ids)
            return Collections.emptyIterator();
        final IgniteGraph graph = (IgniteGraph) this.getTraversal().getGraph().get();        
        return CloseableIteratorUtils.filter(graph.edges(this.ids), edge -> HasContainer.testAll(edge, this.hasContainers));
    }

    private Iterator<? extends Vertex> vertices() {
        if (null == this.ids)
            return Collections.emptyIterator();
        final IgniteGraph graph = (IgniteGraph) this.getTraversal().getGraph().get();
        return lookupVertices(graph, this.hasContainers, this.ids);
    }

    private Iterator<Vertex> lookupVertices(final IgniteGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
        // ids are present, filter on them first
        if (ids.length > 0)
            return CloseableIteratorUtils.filter(graph.vertices(ids), vertex -> HasContainer.testAll(vertex, hasContainers));
        ////// do index lookups //////
        // get a label being search on
        Optional<String> label = hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
                .map(hasContainer -> (String) hasContainer.getValue())
                .findAny();
        if (label.isPresent()) {
        	Optional<RangeGlobalStep<Vertex>> range = this.traversal.getSteps().stream().filter(step -> step instanceof RangeGlobalStep).findAny();
        	int limit = 0;
        	if(range.isPresent()) {
        		limit = (int)range.get().getHighRange();
        	}
        	Optional<HasStep<Element>> kv = this.traversal.getSteps().stream().filter(step -> step instanceof HasStep).findAny();
        	if(kv.isPresent()) {
        		List<HasContainer> hasValue = kv.get().getHasContainers();
        		for(HasContainer hasC: hasValue) {
        			if(hasC.getBiPredicate() == Compare.eq) {
        				return IteratorUtils.stream(graph.verticesByLabel(label.get(),hasC.getKey(),hasC.getValue()))
                                .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        			}
        			else if(hasC.getBiPredicate() == Compare.gt || hasC.getBiPredicate() == Compare.gte) {
        				return IteratorUtils.stream(graph.verticesWithLimit(label.get(),hasC.getKey(),hasC.getValue(),limit))
                                .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        			}
        			else if(hasC.getBiPredicate() == Compare.lt || hasC.getBiPredicate() == Compare.lte) {
        				return IteratorUtils.stream(graph.verticesInRange(label.get(),hasC.getKey(),null, hasC.getValue()))
                                .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        			}
        		}
        		
        	}
        	
        	if(range.isPresent()) {
        		// find a vertex by label with limit
        		RangeGlobalStep<Vertex> rangeValue = range.get();
                return IteratorUtils.stream(graph.verticesByLabel(label.get(),(int)rangeValue.getLowRange(),(int)rangeValue.getHighRange()))
                        .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        	}
            // find a vertex by label
            return IteratorUtils.stream(graph.verticesByLabel(label.get(),0,-1))
                    .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        } else {
            // linear scan
            return CloseableIteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll(vertex, hasContainers));
        }
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else
            this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
