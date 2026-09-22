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
import java.util.Collection;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.WildcardType;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MessageProcessor.CACHE_OBJECT_CLS;
import static org.apache.ignite.internal.MessageProcessor.IGNITE_CHECKED_EXCEPTION_CLS;

/**
 * Generates a {@code *Deployer} class for messages whose {@code @Order} fields have a type that indicates deployment
 * need: {@code CacheObject} subtypes, {@code Collection<? extends CacheObject>}, {@code Iterable<IgniteTxEntry>}, or a
 * nested {@code GridCacheMessage} (whose deployment is delegated). The strategy is inferred entirely from the field type.
 *
 * <p>A message with deployment logic that cannot be inferred from field types implements {@code DeployableMessage};
 * the generated deployer then also delegates to its {@code deploy}, mirroring {@code marshal}.
 */
public class MessageDeploymentGenerator extends MessageCompanionGenerator {
    /** */
    private static final String GRID_CACHE_MESSAGE_CLS = "org.apache.ignite.internal.processors.cache.GridCacheMessage";

    /** */
    private static final String DEPLOYABLE_MESSAGE_CLS = "org.apache.ignite.internal.processors.cache.DeployableMessage";

    /** */
    private static final String IGNITE_TX_ENTRY_CLS = "org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry";

    /** Interface the generated deployers implement. */
    private static final String GRID_CACHE_MESSAGE_DEPLOYER_CLS = "org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer";

    /** */
    private static final String GRID_CACHE_SHARED_CONTEXT_CLS = "org.apache.ignite.internal.processors.cache.GridCacheSharedContext";

    /** */
    private static final String GRID_CACHE_CONTEXT_CLS = "org.apache.ignite.internal.processors.cache.GridCacheContext";

    /** */
    private final TypeMirror gridCacheMsgType;

    /** */
    private final TypeMirror deployableMsgType;

    /** */
    private final TypeMirror cacheObjType;

    /** */
    private final TypeMirror txEntryType;

    /** */
    private final TypeMirror colType;

    /** */
    private final TypeMirror iterableType;

    /** Accumulated source lines for the generated {@code GridCacheMessageDeployer#deploy} implementation. */
    private final List<String> deploy = new ArrayList<>();

    /** */
    private boolean needsCctx;

    /** */
    MessageDeploymentGenerator(ProcessingEnvironment env) {
        super(env);

        gridCacheMsgType = type(GRID_CACHE_MESSAGE_CLS);
        deployableMsgType = type(DEPLOYABLE_MESSAGE_CLS);
        cacheObjType = type(CACHE_OBJECT_CLS);
        txEntryType = type(IGNITE_TX_ENTRY_CLS);
        colType = erasedType(type(Collection.class.getName()));
        iterableType = erasedType(type(Iterable.class.getName()));
    }

    /** {@inheritDoc} */
    @Override protected String typeSuffix() {
        return "Deployer";
    }

    /** {@inheritDoc} */
    @Override protected boolean shouldSkip(TypeElement type, List<VariableElement> fields) {
        if (gridCacheMsgType == null || !assignableFrom(type.asType(), gridCacheMsgType))
            return true;

        // Generate when the message carries custom deployment logic...
        if (hasCustomDeployment(type))
            return false;

        // ...or has any field whose deployment can be inferred from its type.
        for (VariableElement f : fields) {
            if (deployKind(f) != null)
                return false;
        }

        return true;
    }

    /** @return {@code true} if {@code type} implements {@code DeployableMessage} (has hand-written {@code deploy}). */
    private boolean hasCustomDeployment(TypeElement type) {
        return deployableMsgType != null && assignableFrom(type.asType(), deployableMsgType);
    }

    /** {@inheritDoc} */
    @Override protected void generateBody(List<VariableElement> fields) {
        emitMethod(deploy, "deploy(" + simpleNameWithGeneric(type) + " msg, GridCacheSharedContext<?, ?> ctx)", body -> {
            List<String> fieldStmts = new ArrayList<>();

            for (VariableElement field : fields) {
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
    @Override protected String buildClassCode(String deployerClsName) throws IOException {
        try (Writer writer = new StringWriter()) {
            imports.add(type.toString());
            imports.add(IGNITE_CHECKED_EXCEPTION_CLS);
            imports.add(GRID_CACHE_MESSAGE_DEPLOYER_CLS);
            imports.add(GRID_CACHE_SHARED_CONTEXT_CLS);

            if (needsCctx)
                imports.add(GRID_CACHE_CONTEXT_CLS);

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
        if (isCacheIdAwareMessage(type))
            return "GridCacheContext<?, ?> cctx = ctx.cacheContext(msg.cacheId());";

        throw new IllegalStateException("Cannot resolve cache context for " + type.getQualifiedName()
            + ": CacheObject deployment fields are supported for CacheIdAware messages only.");
    }

    /** Returns the deployment strategy for {@code field} based on its Java type, or {@code null} if not deployable. */
    private @Nullable DeployKind deployKind(VariableElement field) {
        TypeMirror fieldType = field.asType();

        if (fieldType.getKind() == TypeKind.ARRAY || fieldType.getKind().isPrimitive())
            return null;

        if (assignableFrom(fieldType, cacheObjType))
            return DeployKind.CACHE_OBJECT;

        if (!(fieldType instanceof DeclaredType))
            return null;

        List<? extends TypeMirror> args = ((DeclaredType)fieldType).getTypeArguments();
        TypeMirror erased = erasedType(fieldType);

        if (!args.isEmpty()) {
            TypeMirror elemType = erasedType(elementBound(args.get(0)));

            if (assignableFrom(erased, colType)
                && assignableFrom(elemType, cacheObjType))
                return DeployKind.CACHE_OBJECTS;

            if (txEntryType != null && iterableType != null
                && assignableFrom(erased, iterableType)
                && env.getTypeUtils().isSameType(elemType, txEntryType))
                return DeployKind.TX_ENTRIES;
        }

        // A nested message field delegates its own deployment (a no-op when that message has no deployer).
        if (gridCacheMsgType != null && assignableFrom(fieldType, gridCacheMsgType))
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
