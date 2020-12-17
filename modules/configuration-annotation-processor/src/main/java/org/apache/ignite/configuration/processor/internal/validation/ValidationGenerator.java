/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration.processor.internal.validation;

import com.squareup.javapoet.CodeBlock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.MirroredTypesException;
import javax.lang.model.type.TypeMirror;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.annotation.Validate;
import org.apache.ignite.configuration.internal.validation.MaxValidator;
import org.apache.ignite.configuration.internal.validation.MinValidator;
import org.apache.ignite.configuration.internal.validation.NotNullValidator;
import org.apache.ignite.configuration.processor.internal.ProcessorException;

/**
 * Class that handles validation generation.
 */
public class ValidationGenerator {
    /**
     * Private constructor.
     */
    private ValidationGenerator() {
    }

    /**
     * Generate validation block.
     *
     * @param variableElement Configuration field.
     * @return Code block for field validation.
     */
    public static CodeBlock generateValidators(VariableElement variableElement) {
        List<CodeBlock> validators = new ArrayList<>();

        processMin(variableElement, validators);

        processMax(variableElement, validators);

        processNotNull(variableElement, validators);

        processCustomValidations(variableElement, validators);

        String text = validators.stream().map(v -> "$L").collect(Collectors.joining(","));

        final CodeBlock validatorsArguments = CodeBlock.builder().add(text, validators.toArray()).build();

        return CodeBlock.builder().add("$T.asList($L)", Arrays.class, validatorsArguments).build();
    }

    /**
     * Process {@link Min} annotations.
     * @param variableElement Field.
     * @param validators Validators code blocks.
     */
    private static void processMin(VariableElement variableElement, List<CodeBlock> validators) {
        final Min minAnnotation = variableElement.getAnnotation(Min.class);
        if (minAnnotation != null) {
            final long minValue = minAnnotation.value();
            final String message = minAnnotation.message();
            final CodeBlock build = CodeBlock.builder().add(
                "new $T<$T<?, ?, ?>>($L, $S)", MinValidator.class, DynamicConfiguration.class, minValue, message
            ).build();
            validators.add(build);
        }
    }

    /**
     * Process {@link Max} annotations.
     * @param variableElement Field.
     * @param validators Validators code blocks.
     */
    private static void processMax(VariableElement variableElement, List<CodeBlock> validators) {
        final Max maxAnnotation = variableElement.getAnnotation(Max.class);

        if (maxAnnotation != null) {
            final long maxValue = maxAnnotation.value();
            final String message = maxAnnotation.message();

            // new MaxValidator
            final CodeBlock build = CodeBlock.builder().add(
                "new $T<$T<?, ?, ?>>($L, $S)", MaxValidator.class, DynamicConfiguration.class, maxValue, message
            ).build();

            validators.add(build);
        }
    }

    /**
     * Process {@link NotNull} annotation.
     * @param variableElement Field.
     * @param validators Validators code blocks.
     */
    private static void processNotNull(VariableElement variableElement, List<CodeBlock> validators) {
        final NotNull notNull = variableElement.getAnnotation(NotNull.class);

        if (notNull != null) {
            final String message = notNull.message();

            final CodeBlock build = CodeBlock.builder().add(
                "new $T<$T<?, ?, ?>>($S)", NotNullValidator.class, DynamicConfiguration.class, message
            ).build();

            validators.add(build);
        }
    }

    /**
     * Process custom validations from {@link Validate} annotation.
     * @param variableElement Field.
     * @param validators Validators code blocks.
     */
    private static void processCustomValidations(VariableElement variableElement, List<CodeBlock> validators) {
        List<Validate> validateAnnotations = new ArrayList<>();

        // There can repeatable Validate annotation, hence Validate.List
        final Validate.List validateAnnotationsList = variableElement.getAnnotation(Validate.List.class);

        if (validateAnnotationsList != null)
            validateAnnotations.addAll(Arrays.asList(validateAnnotationsList.value()));

        // Or there is single Validate annotation
        final Validate validateAnnotationSingle = variableElement.getAnnotation(Validate.class);

        if (validateAnnotationSingle != null)
            validateAnnotations.add(validateAnnotationSingle);

        for (Validate validateAnnotation : validateAnnotations) {
            List<? extends TypeMirror> values = null;
            try {
                //  From JavaDocs: The annotation returned by this method could contain an element whose value is of type Class.
                //  This value cannot be returned directly: information necessary to locate and load a class
                //  (such as the class loader to use) is not available, and the class might not be loadable at all.
                //  Attempting to read a Class object by invoking the relevant method on the returned annotation will
                //  result in a MirroredTypeException, from which the corresponding TypeMirror may be extracted.
                //  Similarly, attempting to read a Class[]-valued element will result in a MirroredTypesException.
                validateAnnotation.value();
            } catch (MirroredTypesException e) {
                values = e.getTypeMirrors();
            }

            if (values == null)
                throw new ProcessorException("Failed to retrieve Validate annotation value");

            for (TypeMirror value : values) {
                final String message = validateAnnotation.message();
                final CodeBlock build = CodeBlock.builder().add("new $T($S)", value, message).build();
                validators.add(build);
            }
        }
    }

}
