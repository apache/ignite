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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * A message is finish-unmarshalled in two passes, cache-free and cache-aware, and {@code MessageUnmarshalOnceCheck}
 * allows both. A field restored by assignment survives that: assigning the same value twice is a no-op. A collection
 * or map one does not: {@code add}/{@code put} run twice double the content.
 *
 * <p>Hence the rule: the cache-free {@code unmarshal(msg, kctx)} of a generated marshaller must not call
 * {@link Collection#add} or {@link Map#put} — the generator emits those in the cache-aware pass only.
 */
public class MarshallerCacheFreeUnmarshalTest {
    /**
     * The two-arg, cache-free {@code unmarshal} overload. The cache-aware overload takes a cache context and a
     * class loader (four args); {@code unmarshalNio} shares the two-arg shape, so the name is matched too.
     */
    private static final DescribedPredicate<JavaCodeUnit> CACHE_FREE_UNMARSHAL =
        new DescribedPredicate<>("cache-free unmarshal(msg, kctx)") {
            @Override public boolean test(JavaCodeUnit unit) {
                List<JavaClass> params = unit.getRawParameterTypes();

                return "unmarshal".equals(unit.getName())
                    && params.size() == 2
                    && params.get(1).isEquivalentTo(GridKernalContext.class);
            }
        };

    /** A {@link Collection#add} or {@link Map#put} append made from within the cache-free {@code unmarshal}. */
    private static final DescribedPredicate<JavaMethodCall> CACHE_FREE_UNMARSHAL_APPEND =
        new DescribedPredicate<>("Collection.add / Map.put from the cache-free unmarshal pass") {
            @Override public boolean test(JavaMethodCall call) {
                if (!CACHE_FREE_UNMARSHAL.test(call.getOrigin()))
                    return false;

                JavaClass owner = call.getTarget().getOwner();
                String mtd = call.getTarget().getName();

                return owner.isAssignableTo(Collection.class) && "add".equals(mtd)
                    || owner.isAssignableTo(Map.class) && "put".equals(mtd);
            }
        };

    /** All production classes on the classpath (the generated marshallers among them), excluding JARs. */
    private static JavaClasses classes;

    /** */
    @BeforeClass
    public static void importClasses() {
        classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
            .importPackages("org.apache.ignite");
    }

    /** The cache-free {@code unmarshal} overload must not append to collections/maps; those are cache-pass only. */
    @Test
    public void cacheFreeFinishDoesNotAppendToCollections() {
        ArchRule rule = noClasses()
            .that()
                .areAssignableTo(MessageMarshaller.class)
            .should()
                .callMethodWhere(CACHE_FREE_UNMARSHAL_APPEND)
            .because("Appends into a collection or map are non-idempotent and belong to the cache-aware " +
                "unmarshal pass only; in the cache-free one they would double-add when both passes run, which " +
                "MessageUnmarshalOnceCheck permits by design and cannot catch.");

        rule.check(classes);
    }
}
