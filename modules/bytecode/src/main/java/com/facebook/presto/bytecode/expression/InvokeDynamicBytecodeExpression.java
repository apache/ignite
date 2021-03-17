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
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

class InvokeDynamicBytecodeExpression
    extends BytecodeExpression {
    private final Method bootstrapMethod;
    private final List<Object> bootstrapArgs;
    private final String methodName;
    private final ParameterizedType returnType;
    private final List<BytecodeExpression> parameters;
    private final List<ParameterizedType> parameterTypes;

    InvokeDynamicBytecodeExpression(
        Method bootstrapMethod,
        Collection<?> bootstrapArgs,
        String methodName,
        ParameterizedType returnType,
        Collection<? extends BytecodeExpression> parameters,
        Collection<ParameterizedType> parameterTypes
    ) {
        super(returnType);
        this.bootstrapMethod = requireNonNull(bootstrapMethod, "bootstrapMethod is null");
        this.bootstrapArgs = List.copyOf(requireNonNull(bootstrapArgs, "bootstrapArgs is null"));
        this.methodName = requireNonNull(methodName, "methodName is null");
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.parameters = List.copyOf(requireNonNull(parameters, "parameters is null"));
        this.parameterTypes = List.copyOf(requireNonNull(parameterTypes, "parameterTypes is null"));
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext) {
        BytecodeBlock block = new BytecodeBlock();
        for (BytecodeExpression parameter : parameters) {
            block.append(parameter);
        }
        return block.invokeDynamic(methodName, returnType, parameterTypes, bootstrapMethod, bootstrapArgs);
    }

    @Override
    public List<BytecodeNode> getChildNodes() {
        return List.of();
    }

    @Override
    protected String formatOneLine() {
        StringBuilder builder = new StringBuilder();
        builder.append("[").append(bootstrapMethod.getName());
        if (!bootstrapArgs.isEmpty()) {
            builder.append("(").append(bootstrapArgs.stream().map(ConstantBytecodeExpression::renderConstant)
                .collect(Collectors.joining(", "))).append(")");
        }
        builder.append("]=>");

        builder.append(methodName)
            .append("(")
            .append(parameters.stream().map(BytecodeExpression::toString).collect(Collectors.joining(", ")))
            .append(")");

        return builder.toString();
    }
}
