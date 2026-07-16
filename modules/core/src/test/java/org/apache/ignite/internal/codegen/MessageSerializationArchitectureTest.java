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

package org.apache.ignite.internal.codegen;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.properties.HasOwner;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.apache.ignite.internal.managers.communication.MessageMarshalling;
import org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer;
import org.apache.ignite.internal.util.nio.MessageSerialization;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.tngtech.archunit.core.domain.JavaCall.Predicates.target;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Verifies that instance methods of {@link MessageSerializer}, {@link MessageMarshaller} and
 * {@link GridCacheMessageDeployer} are only called from classes that implement these interfaces (i.e. generated
 * serializers/marshallers/deployers and their hand-written wrappers). All other code must use the static
 * convenience methods:
 * <ul>
 *     <li>{@link MessageSerialization#writeTo}</li>
 *     <li>{@link MessageSerialization#readFrom}</li>
 *     <li>{@link MessageMarshalling#marshal}</li>
 *     <li>{@code MessageMarshalling.unmarshal}</li>
 *     <li>static {@code GridCacheMessageDeployer.deploy(factory, msg, ctx)}</li>
 * </ul>
 *
 * <p>The rules key on whether the called method is {@code static}, not on its name — so any instance method added
 * to these interfaces is covered automatically.
 */
public class MessageSerializationArchitectureTest {
    /** Matches method calls that resolve to a non-static (instance) method. */
    private static final DescribedPredicate<JavaMethodCall> TO_INSTANCE_METHOD =
        new DescribedPredicate<>("to instance method") {
            @Override public boolean test(JavaMethodCall call) {
                return call.getTarget().resolveMember()
                    .map(m -> !m.getModifiers().contains(JavaModifier.STATIC))
                    .orElse(false);
            }
        };

    /** Classes under analysis: all production + test sources on the classpath, excluding JARs. */
    private static JavaClasses classes;

    /** */
    @BeforeClass
    public static void importClasses() {
        classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
            .importPackages("org.apache.ignite");
    }

    /**
     * Instance methods of {@link MessageSerializer} ({@code writeTo}, {@code readFrom}) must only be
     * called from within classes that themselves implement {@link MessageSerializer} — i.e. generated
     * serializers and hand-written wrappers that delegate to the underlying serializer — or the
     * {@link MessageSerialization} dispatcher that resolves and calls them.
     *
     * Everyone else must use the static {@link MessageSerialization} entry points.
     */
    @Test
    public void serializerInstanceMethodsOnlyCalledFromImplementations() {
        ArchRule rule = noClasses()
            .that()
                // Exclude MessageSerializer implementations (generated + wrappers) and the dispatch facade.
                .areNotAssignableTo(MessageSerializer.class)
                .and().areNotAssignableTo(MessageSerialization.class)
            .should()
                .callMethodWhere(TO_INSTANCE_METHOD
                        .and(target(HasOwner.Predicates.With.owner(assignableTo(MessageSerializer.class))))
                )
            .because("Use static MessageSerialization.writeTo(factory, msg, writer) and " +
                "MessageSerialization.readFrom(factory, msg, reader) instead of calling instance methods directly.");

        rule.check(classes);
    }

    /**
     * Instance methods of {@link MessageMarshaller} ({@code marshal}, {@code unmarshal}) must
     * only be called from within classes that themselves implement {@link MessageMarshaller} — i.e. generated
     * marshallers and hand-written wrappers that delegate to the underlying marshaller — or the
     * {@link MessageMarshalling} dispatcher that resolves and calls them.
     *
     * Everyone else must use the static {@link MessageMarshalling} entry points.
     */
    @Test
    public void marshallerInstanceMethodsOnlyCalledFromImplementations() {
        ArchRule rule = noClasses()
            .that()
                // Exclude MessageMarshaller implementations (generated + wrappers) and the MessageMarshalling dispatcher.
                .areNotAssignableTo(MessageMarshaller.class)
                .and().areNotAssignableTo(MessageMarshalling.class)
            .should()
                .callMethodWhere(TO_INSTANCE_METHOD
                        .and(target(HasOwner.Predicates.With.owner(assignableTo(MessageMarshaller.class))))
                )
            .because("Use the static MessageMarshalling.marshal(...) / MessageMarshalling.unmarshal(...) entry points " +
                "instead of calling instance methods directly.");

        rule.check(classes);
    }

    /**
     * Instance method of {@link GridCacheMessageDeployer} ({@code deploy}) must only be called from
     * within classes that themselves implement {@link GridCacheMessageDeployer} — i.e. generated deployers.
     *
     * Everyone else must use the static {@code GridCacheMessageDeployer.deploy(factory, msg, ctx)} facade.
     */
    @Test
    public void deployerInstanceMethodOnlyCalledFromImplementations() {
        ArchRule rule = noClasses()
            .that()
                // Exclude GridCacheMessageDeployer itself and all its implementations (generated deployers).
                .areNotAssignableTo(GridCacheMessageDeployer.class)
            .should()
                .callMethodWhere(TO_INSTANCE_METHOD
                        .and(target(HasOwner.Predicates.With.owner(assignableTo(GridCacheMessageDeployer.class))))
                )
            .because("Use static GridCacheMessageDeployer.deploy(factory, msg, ctx) instead of " +
                "calling the instance method directly.");

        rule.check(classes);
    }
}
