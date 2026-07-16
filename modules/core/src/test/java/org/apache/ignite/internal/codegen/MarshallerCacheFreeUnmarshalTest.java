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
 * A message is finish-unmarshalled in two passes — cache-free (via {@code GridIoManager}) and cache-aware (via a
 * subsystem) — and the mode-aware no-double-unmarshal check ({@link MessageMarshaller.Dedup}) deliberately allows
 * both. So whatever the cache-free pass does must stay correct when the cache-aware pass runs it again.
 *
 * <p>The distinction is the side effect. A plain {@code @Marshalled} field unmarshals by <b>assignment</b>
 * ({@code msg.x = ...}); assigning the same value twice is a no-op, so it is idempotent and needs no guarding. A
 * {@code @Marshalled} / {@code @Marshalled} field unmarshals by <b>mutation</b> ({@code collection.add} /
 * {@code map.put}); running that twice appends the elements twice — a doubled, corrupt collection. So a collection/map
 * mutation is the one thing that must not run in both passes, and the generator keeps it in the cache-aware pass only.
 *
 * <p>That is exactly what this rule checks — purely the mutating side effect, not the fields: the cache-free
 * {@code unmarshal(msg, kctx)} overload of every generated marshaller must not call {@link Collection#add} or
 * {@link Map#put}. Assignments are left unchecked (they can't break); a generator regression that moved an append into
 * the cache-free pass — a real double-add the runtime check can't see, since it allows both passes — fails here.
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
            .because("@Marshalled/@Marshalled appends are non-idempotent and run only in the cache-aware " +
                "unmarshal pass; an append in the cache-free pass would double-add when both passes run, which " +
                "the mode-aware unmarshal-once check (MessageMarshaller.Dedup) permits and cannot catch.");

        rule.check(classes);
    }
}
