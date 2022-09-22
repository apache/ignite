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
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

public final class IgniteVertexStep<E extends Element> extends VertexStep<E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public IgniteVertexStep(final VertexStep<E> originalVertexStep) {
        super(
                originalVertexStep.getTraversal(), originalVertexStep.getReturnClass(),
                originalVertexStep.getDirection(), originalVertexStep.getEdgeLabels());

        originalVertexStep.getLabels().forEach(this::addLabel);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        return Vertex.class.isAssignableFrom(getReturnClass()) ?
                (Iterator<E>) lookupVertices(traverser, this.hasContainers) :
                (Iterator<E>) lookupEdges(traverser, this.hasContainers);
    }

    private Iterator<Vertex> lookupVertices(final Traverser.Admin<Vertex> traverser, final List<HasContainer> hasContainers) {
        // linear scan
        return CloseableIteratorUtils.filter(traverser.get().vertices(getDirection(), getEdgeLabels()),
                vertex -> HasContainer.testAll(vertex, hasContainers));
    }

    private Iterator<Edge> lookupEdges(final Traverser.Admin<Vertex> traverser, final List<HasContainer> hasContainers) {
        // linear scan
        return CloseableIteratorUtils.filter(traverser.get().edges(getDirection(), getEdgeLabels()),
                edge -> HasContainer.testAll(edge, hasContainers));
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return StringFactory.stepString(this, getDirection(), Arrays.asList(getEdgeLabels()), getReturnClass().getSimpleName().toLowerCase(), this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
