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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.WildcardType;
import javax.lang.model.util.ElementFilter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.systemview.SystemViewRowAttributeWalkerProcessor.superclasses;

/**
 * Generates a {@code *Deployer} class for messages whose {@code @Order} fields have a type that indicates deployment
 * need: {@code CacheObject} subtypes, {@code Collection<? extends CacheObject>}, {@code Iterable<IgniteTxEntry>}, or a
 * nested {@code GridCacheMessage} (whose deployment is delegated). The strategy is inferred entirely from the field type.
 *
 * <p>A message with deployment logic that cannot be inferred from field types implements {@code DeployableMessage};
 * the generated deployer then also delegates to its {@code deploy}, mirroring {@code marshal}.
 */
public class MessageDeploymentGenerator extends MessageGenerator {
    /** FQN of GridCacheMessage; hierarchy scan stops here (exclusive). */
    private static final String GRID_CACHE_MESSAGE = "org.apache.ignite.internal.processors.cache.GridCacheMessage";

    /** */
    private final TypeMirror cacheIdMsgMirror;

    /** */
    private final TypeMirror cacheGroupIdMsgMirror;

    /** */
    private final TypeMirror gridCacheMessageMirror;

    /** */
    private final TypeMirror deployableMessageMirror;

    /** */
    private final TypeMirror cacheObjectMirror;

    /** */
    private final TypeMirror txEntryMirror;

    /** */
    private final TypeMirror collectionMirror;

    /** */
    private final TypeMirror iterableMirror;

    /** Accumulated source lines for the generated {@code deploy} method. */
    private final List<String> deploy = new ArrayList<>();

    /** */
    private boolean needsCctx;

    /** */
    MessageDeploymentGenerator(ProcessingEnvironment env) {
        super(env);

        cacheIdMsgMirror = type("org.apache.ignite.internal.processors.cache.GridCacheIdMessage");
        cacheGroupIdMsgMirror = type("org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage");
        gridCacheMessageMirror = type(GRID_CACHE_MESSAGE);
        deployableMessageMirror = type("org.apache.ignite.internal.processors.cache.DeployableMessage");
        cacheObjectMirror = type("org.apache.ignite.internal.processors.cache.CacheObject");
        txEntryMirror = type("org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry");
        collectionMirror = erasedType(type("java.util.Collection"));
        iterableMirror = erasedType(type("java.lang.Iterable"));
    }

    /** {@inheritDoc} */
    @Override String typeSuffix() {
        return "Deployer";
    }

    /** {@inheritDoc} */
    @Override boolean shouldSkip(TypeElement type) {
        if (gridCacheMessageMirror == null || !assignableFrom(type.asType(), gridCacheMessageMirror))
            return true;

        // Generate when the message carries custom deployment logic...
        if (hasCustomDeployment(type))
            return false;

        // ...or has any field whose deployment can be inferred from its type.
        for (VariableElement f : allHierarchyFields(type)) {
            if (deployKind(f) != null)
                return false;
        }

        return true;
    }

    /** @return {@code true} if {@code type} implements {@code DeployableMessage} (has hand-written {@code deploy}). */
    private boolean hasCustomDeployment(TypeElement type) {
        return deployableMessageMirror != null && assignableFrom(type.asType(), deployableMessageMirror);
    }

    /** {@inheritDoc} */
    @Override void generateBody(List<VariableElement> fields) {
        emitMethod(deploy, "deploy(" + simpleNameWithGeneric(type) + " msg, GridCacheSharedContext<?, ?> ctx)", body -> {
            List<String> fieldStmts = new ArrayList<>();

            for (VariableElement field : allHierarchyFields(type)) {
                DeployKind kind = deployKind(field);

                if (kind == null)
                    continue;

                needsCctx |= kind.needsCctx;

                appendBlock(fieldStmts, List.of(indentedLine(kind.stmt, fieldAccessor(field))));
            }

            if (needsCctx) {
                body.add(indentedLine(cctxResolutionLine()));
                body.add(EMPTY);
            }

            body.addAll(fieldStmts);

            // Delegate the non-inferable part to the message's own deploy, mirroring msg.marshal().
            if (hasCustomDeployment(type)) {
                if (!fieldStmts.isEmpty())
                    body.add(EMPTY);

                body.add(indentedLine("msg.deploy(ctx);"));
            }
        });
    }

    /** {@inheritDoc} */
    @Override String buildClassCode(String deployerClsName) throws IOException {
        try (Writer writer = new StringWriter()) {
            imports.add(type.toString());
            imports.add("org.apache.ignite.IgniteCheckedException");
            imports.add("org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer");
            imports.add("org.apache.ignite.internal.processors.cache.GridCacheSharedContext");

            if (needsCctx)
                imports.add("org.apache.ignite.internal.processors.cache.GridCacheContext");

            writeClassHeader(writer, "GridCacheMessageDeployer", deployerClsName);

            writer.write(" {" + NL);

            for (String line : deploy)
                writer.write(line + NL);

            writer.write("}");

            return writer.toString();
        }
    }

    /** Returns the line that resolves {@code cctx} from {@code ctx} based on the message type hierarchy. */
    private String cctxResolutionLine() {
        if (assignableFrom(type.asType(), cacheGroupIdMsgMirror))
            return "GridCacheContext<?, ?> cctx = ctx.cacheContext(msg.groupId());";

        if (assignableFrom(type.asType(), cacheIdMsgMirror))
            return "GridCacheContext<?, ?> cctx = ctx.cacheContext(msg.cacheId());";

        throw new IllegalStateException("Cannot resolve cache context for " + type.getQualifiedName()
            + ": message has CacheObject field(s) but is neither GridCacheIdMessage nor GridCacheGroupIdMessage.");
    }

    /** All fields declared across the class hierarchy of {@code te}, up to but excluding {@code Object}. */
    private List<VariableElement> allHierarchyFields(TypeElement te) {
        List<VariableElement> result = new ArrayList<>();

        superclasses(env, te).forEach(c -> result.addAll(ElementFilter.fieldsIn(c.getEnclosedElements())));

        return result;
    }

    /** Returns the deployment strategy for {@code field} based on its Java type, or {@code null} if not deployable. */
    private @Nullable DeployKind deployKind(VariableElement field) {
        // Only serialized fields are sent over the wire and thus need deployment; transient fields are skipped.
        if (field.getAnnotation(Order.class) == null)
            return null;

        TypeMirror fieldType = field.asType();

        if (fieldType.getKind() == TypeKind.ARRAY || fieldType.getKind().isPrimitive())
            return null;

        if (assignableFrom(fieldType, cacheObjectMirror))
            return DeployKind.CACHE_OBJECT;

        if (!(fieldType instanceof DeclaredType))
            return null;

        List<? extends TypeMirror> args = ((DeclaredType)fieldType).getTypeArguments();
        TypeMirror erased = erasedType(fieldType);

        if (!args.isEmpty()) {
            TypeMirror elemType = erasedType(elementBound(args.get(0)));

            if (assignableFrom(erased, collectionMirror)
                && assignableFrom(elemType, cacheObjectMirror))
                return DeployKind.CACHE_OBJECTS;

            if (txEntryMirror != null && iterableMirror != null
                && assignableFrom(erased, iterableMirror)
                && env.getTypeUtils().isSameType(elemType, txEntryMirror))
                return DeployKind.TX_ENTRIES;
        }

        // A nested message field delegates its own deployment (a no-op when that message has no deployer).
        if (gridCacheMessageMirror != null && assignableFrom(fieldType, gridCacheMessageMirror))
            return DeployKind.NESTED;

        return null;
    }

    /** Unwraps the upper bound of a wildcard type; returns the type as-is for non-wildcards. */
    private TypeMirror elementBound(TypeMirror arg) {
        if (arg instanceof WildcardType) {
            TypeMirror bound = ((WildcardType)arg).getExtendsBound();
            return bound != null ? bound : arg;
        }

        return arg;
    }

    /** Deployment strategy inferred from a field's type. */
    private enum DeployKind {
        /** Single {@code CacheObject} field. */
        CACHE_OBJECT("msg.deployCacheObject(%s, cctx);", true),

        /** {@code Collection} of {@code CacheObject}s. */
        CACHE_OBJECTS("msg.deployCacheObjects(%s, cctx);", true),

        /** {@code Iterable<IgniteTxEntry>}. */
        TX_ENTRIES("msg.deployTx(%s, ctx);", false),

        /** Nested {@code GridCacheMessage} delegating its own deployment. */
        NESTED("GridCacheMessageDeployer.deploy(ctx.kernalContext().messageFactory(), %s, ctx);", false);

        /** Statement template with {@code %s} placeholder for the field accessor. */
        private final String stmt;

        /** Whether the statement requires the resolved {@code cctx}. */
        private final boolean needsCctx;

        /** */
        DeployKind(String stmt, boolean needsCctx) {
            this.stmt = stmt;
            this.needsCctx = needsCctx;
        }
    }
}
