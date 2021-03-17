/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.bytecode.instruction;

import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.BytecodeVisitor;
import com.facebook.presto.bytecode.MethodGenerationContext;
import java.util.List;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

public class LabelNode
    implements InstructionNode {
    private final String name;
    private final Label label;

    public LabelNode() {
        this.label = new Label();
        this.name = "label@" + label.hashCode();
    }

    public LabelNode(LabelNode labelNode) {
        this.label = labelNode.getLabel();
        this.name = "label@" + label.hashCode();
    }

    public LabelNode(String name) {
        this.label = new Label();
        this.name = name + "@" + label.hashCode();
    }

    public String getName() {
        return name;
    }

    public Label getLabel() {
        return label;
    }

    @Override
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext) {
        visitor.visitLabel(label);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{name=" + name + '}';
    }

    @Override
    public List<BytecodeNode> getChildNodes() {
        return List.of();
    }

    @Override
    public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor) {
        return visitor.visitLabel(parent, this);
    }
}
