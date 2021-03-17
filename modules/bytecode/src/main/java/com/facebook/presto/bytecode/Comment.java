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
package com.facebook.presto.bytecode;

import com.facebook.presto.bytecode.instruction.InstructionNode;
import java.util.List;
import org.objectweb.asm.MethodVisitor;

public class Comment
    implements InstructionNode {
    protected final String comment;

    public Comment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext) {
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '{' + comment + '}';
    }

    @Override
    public List<BytecodeNode> getChildNodes() {
        return List.of();
    }

    @Override
    public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor) {
        return visitor.visitComment(parent, this);
    }
}
