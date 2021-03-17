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
package com.facebook.presto.bytecode.expression;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;

import static com.facebook.presto.bytecode.BytecodeUtils.checkArgument;
import static java.util.Objects.requireNonNull;

class InvokeBytecodeExpression
    extends BytecodeExpression {
    public static InvokeBytecodeExpression createInvoke(
        BytecodeExpression instance,
        String methodName,
        ParameterizedType returnType,
        Collection<ParameterizedType> parameterTypes,
        Collection<? extends BytecodeExpression> parameters) {
        return new InvokeBytecodeExpression(
            requireNonNull(instance, "instance is null"),
            instance.getType(),
            requireNonNull(methodName, "methodName is null"),
            requireNonNull(returnType, "returnType is null"),
            requireNonNull(parameterTypes, "parameterTypes is null"),
            requireNonNull(parameters, "parameters is null"));
    }

    @Nullable
    private final BytecodeExpression instance;
    private final ParameterizedType methodTargetType;
    private final String methodName;
    private final ParameterizedType returnType;
    private final List<BytecodeExpression> parameters;
    private final List<ParameterizedType> parameterTypes;

    InvokeBytecodeExpression(
        @Nullable BytecodeExpression instance,
        ParameterizedType methodTargetType,
        String methodName,
        ParameterizedType returnType,
        Collection<ParameterizedType> parameterTypes,
        Collection<? extends BytecodeExpression> parameters) {
        super(requireNonNull(returnType, "returnType is null"));
        checkArgument(instance == null || !instance.getType().isPrimitive(), "Type %s does not have methods", getType());
        this.instance = instance;
        this.methodTargetType = requireNonNull(methodTargetType, "methodTargetType is null");
        this.methodName = requireNonNull(methodName, "methodName is null");
        this.returnType = returnType;
        this.parameterTypes = List.copyOf(requireNonNull(parameterTypes, "parameterTypes is null"));
        this.parameters = List.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext) {
        BytecodeBlock block = new BytecodeBlock();
        if (instance != null) {
            block.append(instance);
        }

        for (BytecodeExpression parameter : parameters) {
            block.append(parameter);
        }

        if (instance == null) {
            return block.invokeStatic(methodTargetType, methodName, returnType, parameterTypes);
        }
        else if (instance.getType().isInterface()) {
            return block.invokeInterface(methodTargetType, methodName, returnType, parameterTypes);
        }
        else {
            return block.invokeVirtual(methodTargetType, methodName, returnType, parameterTypes);
        }
    }

    @Override
    protected String formatOneLine() {
        if (instance == null) {
            return methodTargetType.getSimpleName() + "." + methodName + "(" +
                parameters.stream().map(BytecodeExpression::toString).collect(Collectors.joining(", ")) + ")";
        }

        return instance + "." + methodName + "(" +
            parameters.stream().map(BytecodeExpression::toString).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public List<BytecodeNode> getChildNodes() {
        if (instance == null)
            return List.copyOf(parameters);

        final ArrayList<BytecodeNode> children = new ArrayList<>(parameters.size() + 1);
        children.add(instance);
        children.addAll(parameters);
        return Collections.unmodifiableList(children);
    }
}
