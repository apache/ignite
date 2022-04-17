/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/core/schematics/migrations/renderer-to-renderer2/migration", ["require", "exports", "typescript", "@angular/core/schematics/migrations/renderer-to-renderer2/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const ts = require("typescript");
    const util_1 = require("@angular/core/schematics/migrations/renderer-to-renderer2/util");
    /** Replaces an import inside an import statement with a different one. */
    function replaceImport(node, oldImport, newImport) {
        const isAlreadyImported = util_1.findImportSpecifier(node.elements, newImport);
        if (isAlreadyImported) {
            return node;
        }
        const existingImport = util_1.findImportSpecifier(node.elements, oldImport);
        if (!existingImport) {
            throw new Error(`Could not find an import to replace using ${oldImport}.`);
        }
        return ts.updateNamedImports(node, [
            ...node.elements.filter(current => current !== existingImport),
            // Create a new import while trying to preserve the alias of the old one.
            ts.createImportSpecifier(existingImport.propertyName ? ts.createIdentifier(newImport) : undefined, existingImport.propertyName ? existingImport.name : ts.createIdentifier(newImport))
        ]);
    }
    exports.replaceImport = replaceImport;
    /**
     * Migrates a function call expression from `Renderer` to `Renderer2`.
     * Returns null if the expression should be dropped.
     */
    function migrateExpression(node, typeChecker) {
        if (isPropertyAccessCallExpression(node)) {
            switch (node.expression.name.getText()) {
                case 'setElementProperty':
                    return { node: renameMethodCall(node, 'setProperty') };
                case 'setText':
                    return { node: renameMethodCall(node, 'setValue') };
                case 'listenGlobal':
                    return { node: renameMethodCall(node, 'listen') };
                case 'selectRootElement':
                    return { node: migrateSelectRootElement(node) };
                case 'setElementClass':
                    return { node: migrateSetElementClass(node) };
                case 'setElementStyle':
                    return { node: migrateSetElementStyle(node, typeChecker) };
                case 'invokeElementMethod':
                    return { node: migrateInvokeElementMethod(node) };
                case 'setBindingDebugInfo':
                    return { node: null };
                case 'createViewRoot':
                    return { node: migrateCreateViewRoot(node) };
                case 'setElementAttribute':
                    return {
                        node: switchToHelperCall(node, "__ngRendererSetElementAttributeHelper" /* setElementAttribute */, node.arguments),
                        requiredHelpers: [
                            "AnyDuringRendererMigration" /* any */, "__ngRendererSplitNamespaceHelper" /* splitNamespace */, "__ngRendererSetElementAttributeHelper" /* setElementAttribute */
                        ]
                    };
                case 'createElement':
                    return {
                        node: switchToHelperCall(node, "__ngRendererCreateElementHelper" /* createElement */, node.arguments.slice(0, 2)),
                        requiredHelpers: ["AnyDuringRendererMigration" /* any */, "__ngRendererSplitNamespaceHelper" /* splitNamespace */, "__ngRendererCreateElementHelper" /* createElement */]
                    };
                case 'createText':
                    return {
                        node: switchToHelperCall(node, "__ngRendererCreateTextHelper" /* createText */, node.arguments.slice(0, 2)),
                        requiredHelpers: ["AnyDuringRendererMigration" /* any */, "__ngRendererCreateTextHelper" /* createText */]
                    };
                case 'createTemplateAnchor':
                    return {
                        node: switchToHelperCall(node, "__ngRendererCreateTemplateAnchorHelper" /* createTemplateAnchor */, node.arguments.slice(0, 1)),
                        requiredHelpers: ["AnyDuringRendererMigration" /* any */, "__ngRendererCreateTemplateAnchorHelper" /* createTemplateAnchor */]
                    };
                case 'projectNodes':
                    return {
                        node: switchToHelperCall(node, "__ngRendererProjectNodesHelper" /* projectNodes */, node.arguments),
                        requiredHelpers: ["AnyDuringRendererMigration" /* any */, "__ngRendererProjectNodesHelper" /* projectNodes */]
                    };
                case 'animate':
                    return {
                        node: migrateAnimateCall(),
                        requiredHelpers: ["AnyDuringRendererMigration" /* any */, "__ngRendererAnimateHelper" /* animate */]
                    };
                case 'destroyView':
                    return {
                        node: switchToHelperCall(node, "__ngRendererDestroyViewHelper" /* destroyView */, [node.arguments[1]]),
                        requiredHelpers: ["AnyDuringRendererMigration" /* any */, "__ngRendererDestroyViewHelper" /* destroyView */]
                    };
                case 'detachView':
                    return {
                        node: switchToHelperCall(node, "__ngRendererDetachViewHelper" /* detachView */, [node.arguments[0]]),
                        requiredHelpers: ["AnyDuringRendererMigration" /* any */, "__ngRendererDetachViewHelper" /* detachView */]
                    };
                case 'attachViewAfter':
                    return {
                        node: switchToHelperCall(node, "__ngRendererAttachViewAfterHelper" /* attachViewAfter */, node.arguments),
                        requiredHelpers: ["AnyDuringRendererMigration" /* any */, "__ngRendererAttachViewAfterHelper" /* attachViewAfter */]
                    };
            }
        }
        return { node };
    }
    exports.migrateExpression = migrateExpression;
    /** Checks whether a node is a PropertyAccessExpression. */
    function isPropertyAccessCallExpression(node) {
        return ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression);
    }
    /** Renames a method call while keeping all of the parameters in place. */
    function renameMethodCall(node, newName) {
        const newExpression = ts.updatePropertyAccess(node.expression, node.expression.expression, ts.createIdentifier(newName));
        return ts.updateCall(node, newExpression, node.typeArguments, node.arguments);
    }
    /**
     * Migrates a `selectRootElement` call by removing the last argument which is no longer supported.
     */
    function migrateSelectRootElement(node) {
        // The only thing we need to do is to drop the last argument
        // (`debugInfo`), if the consumer was passing it in.
        if (node.arguments.length > 1) {
            return ts.updateCall(node, node.expression, node.typeArguments, [node.arguments[0]]);
        }
        return node;
    }
    /**
     * Migrates a call to `setElementClass` either to a call to `addClass` or `removeClass`, or
     * to an expression like `isAdd ? addClass(el, className) : removeClass(el, className)`.
     */
    function migrateSetElementClass(node) {
        // Clone so we don't mutate by accident. Note that we assume that
        // the user's code is providing all three required arguments.
        const outputMethodArgs = node.arguments.slice();
        const isAddArgument = outputMethodArgs.pop();
        const createRendererCall = (isAdd) => {
            const innerExpression = node.expression.expression;
            const topExpression = ts.createPropertyAccess(innerExpression, isAdd ? 'addClass' : 'removeClass');
            return ts.createCall(topExpression, [], node.arguments.slice(0, 2));
        };
        // If the call has the `isAdd` argument as a literal boolean, we can map it directly to
        // `addClass` or `removeClass`. Note that we can't use the type checker here, because it
        // won't tell us whether the value resolves to true or false.
        if (isAddArgument.kind === ts.SyntaxKind.TrueKeyword ||
            isAddArgument.kind === ts.SyntaxKind.FalseKeyword) {
            return createRendererCall(isAddArgument.kind === ts.SyntaxKind.TrueKeyword);
        }
        // Otherwise create a ternary on the variable.
        return ts.createConditional(isAddArgument, createRendererCall(true), createRendererCall(false));
    }
    /**
     * Migrates a call to `setElementStyle` call either to a call to
     * `setStyle` or `removeStyle`. or to an expression like
     * `value == null ? removeStyle(el, key) : setStyle(el, key, value)`.
     */
    function migrateSetElementStyle(node, typeChecker) {
        const args = node.arguments;
        const addMethodName = 'setStyle';
        const removeMethodName = 'removeStyle';
        const lastArgType = args[2] ?
            typeChecker.typeToString(typeChecker.getTypeAtLocation(args[2]), node, ts.TypeFormatFlags.AddUndefined) :
            null;
        // Note that for a literal null, TS considers it a `NullKeyword`,
        // whereas a literal `undefined` is just an Identifier.
        if (args.length === 2 || lastArgType === 'null' || lastArgType === 'undefined') {
            // If we've got a call with two arguments, or one with three arguments where the last one is
            // `undefined` or `null`, we can safely switch to a `removeStyle` call.
            const innerExpression = node.expression.expression;
            const topExpression = ts.createPropertyAccess(innerExpression, removeMethodName);
            return ts.createCall(topExpression, [], args.slice(0, 2));
        }
        else if (args.length === 3) {
            // We need the checks for string literals, because the type of something
            // like `"blue"` is the literal `blue`, not `string`.
            if (lastArgType === 'string' || lastArgType === 'number' || ts.isStringLiteral(args[2]) ||
                ts.isNoSubstitutionTemplateLiteral(args[2]) || ts.isNumericLiteral(args[2])) {
                // If we've got three arguments and the last one is a string literal or a number, we
                // can safely rename to `setStyle`.
                return renameMethodCall(node, addMethodName);
            }
            else {
                // Otherwise migrate to a ternary that looks like:
                // `value == null ? removeStyle(el, key) : setStyle(el, key, value)`
                const condition = ts.createBinary(args[2], ts.SyntaxKind.EqualsEqualsToken, ts.createNull());
                const whenNullCall = renameMethodCall(ts.createCall(node.expression, [], args.slice(0, 2)), removeMethodName);
                return ts.createConditional(condition, whenNullCall, renameMethodCall(node, addMethodName));
            }
        }
        return node;
    }
    /**
     * Migrates a call to `invokeElementMethod(target, method, [arg1, arg2])` either to
     * `target.method(arg1, arg2)` or `(target as any)[method].apply(target, [arg1, arg2])`.
     */
    function migrateInvokeElementMethod(node) {
        const [target, name, args] = node.arguments;
        const isNameStatic = ts.isStringLiteral(name) || ts.isNoSubstitutionTemplateLiteral(name);
        const isArgsStatic = !args || ts.isArrayLiteralExpression(args);
        if (isNameStatic && isArgsStatic) {
            // If the name is a static string and the arguments are an array literal,
            // we can safely convert the node into a call expression.
            const expression = ts.createPropertyAccess(target, name.text);
            const callArguments = args ? args.elements : [];
            return ts.createCall(expression, [], callArguments);
        }
        else {
            // Otherwise create an expression in the form of `(target as any)[name].apply(target, args)`.
            const asExpression = ts.createParen(ts.createAsExpression(target, ts.createKeywordTypeNode(ts.SyntaxKind.AnyKeyword)));
            const elementAccess = ts.createElementAccess(asExpression, name);
            const applyExpression = ts.createPropertyAccess(elementAccess, 'apply');
            return ts.createCall(applyExpression, [], args ? [target, args] : [target]);
        }
    }
    /** Migrates a call to `createViewRoot` to whatever node was passed in as the first argument. */
    function migrateCreateViewRoot(node) {
        return node.arguments[0];
    }
    /** Migrates a call to `migrate` a direct call to the helper. */
    function migrateAnimateCall() {
        return ts.createCall(ts.createIdentifier("__ngRendererAnimateHelper" /* animate */), [], []);
    }
    /**
     * Switches out a call to the `Renderer` to a call to one of our helper functions.
     * Most of the helpers accept an instance of `Renderer2` as the first argument and all
     * subsequent arguments differ.
     * @param node Node of the original method call.
     * @param helper Name of the helper with which to replace the original call.
     * @param args Arguments that should be passed into the helper after the renderer argument.
     */
    function switchToHelperCall(node, helper, args) {
        return ts.createCall(ts.createIdentifier(helper), [], [node.expression.expression, ...args]);
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWlncmF0aW9uLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zY2hlbWF0aWNzL21pZ3JhdGlvbnMvcmVuZGVyZXItdG8tcmVuZGVyZXIyL21pZ3JhdGlvbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILGlDQUFpQztJQUdqQyx5RkFBMkM7SUFLM0MsMEVBQTBFO0lBQzFFLFNBQWdCLGFBQWEsQ0FBQyxJQUFxQixFQUFFLFNBQWlCLEVBQUUsU0FBaUI7UUFDdkYsTUFBTSxpQkFBaUIsR0FBRywwQkFBbUIsQ0FBQyxJQUFJLENBQUMsUUFBUSxFQUFFLFNBQVMsQ0FBQyxDQUFDO1FBRXhFLElBQUksaUJBQWlCLEVBQUU7WUFDckIsT0FBTyxJQUFJLENBQUM7U0FDYjtRQUVELE1BQU0sY0FBYyxHQUFHLDBCQUFtQixDQUFDLElBQUksQ0FBQyxRQUFRLEVBQUUsU0FBUyxDQUFDLENBQUM7UUFFckUsSUFBSSxDQUFDLGNBQWMsRUFBRTtZQUNuQixNQUFNLElBQUksS0FBSyxDQUFDLDZDQUE2QyxTQUFTLEdBQUcsQ0FBQyxDQUFDO1NBQzVFO1FBRUQsT0FBTyxFQUFFLENBQUMsa0JBQWtCLENBQUMsSUFBSSxFQUFFO1lBQ2pDLEdBQUcsSUFBSSxDQUFDLFFBQVEsQ0FBQyxNQUFNLENBQUMsT0FBTyxDQUFDLEVBQUUsQ0FBQyxPQUFPLEtBQUssY0FBYyxDQUFDO1lBQzlELHlFQUF5RTtZQUN6RSxFQUFFLENBQUMscUJBQXFCLENBQ3BCLGNBQWMsQ0FBQyxZQUFZLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxnQkFBZ0IsQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLENBQUMsU0FBUyxFQUN4RSxjQUFjLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQyxjQUFjLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsZ0JBQWdCLENB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Q0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQztTQUM3RTtJQUNILENBQUM7SUFFRCxnR0FBZ0c7SUFDaEcsU0FBUyxxQkFBcUIsQ0FBQyxJQUF1QjtRQUNwRCxPQUFPLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDM0IsQ0FBQztJQUVELGdFQUFnRTtJQUNoRSxTQUFTLGtCQUFrQjtRQUN6QixPQUFPLEVBQUUsQ0FBQyxVQUFVLENBQUMsRUFBRSxDQUFDLGdCQUFnQiwyQ0FBd0IsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDLENBQUM7SUFDNUUsQ0FBQztJQUVEOzs7Ozs7O09BT0c7SUFDSCxTQUFTLGtCQUFrQixDQUN2QixJQUFrQyxFQUFFLE1BQXNCLEVBQzFELElBQW1EO1FBQ3JELE9BQU8sRUFBRSxDQUFDLFVBQVUsQ0FBQyxFQUFFLENBQUMsZ0JBQWdCLENBQUMsTUFBTSxDQUFDLEVBQUUsRUFBRSxFQUFFLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxVQUFVLEVBQUUsR0FBRyxJQUFJLENBQUMsQ0FBQyxDQUFDO0lBQy9GLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCAqIGFzIHRzIGZyb20gJ3R5cGVzY3JpcHQnO1xuXG5pbXBvcnQge0hlbHBlckZ1bmN0aW9ufSBmcm9tICcuL2hlbHBlcnMnO1xuaW1wb3J0IHtmaW5kSW1wb3J0U3BlY2lmaWVyfSBmcm9tICcuL3V0aWwnO1xuXG4vKiogQSBjYWxsIGV4cHJlc3Npb24gdGhhdCBpcyBiYXNlZCBvbiBhIHByb3BlcnR5IGFjY2Vzcy4gKi9cbnR5cGUgUHJvcGVydHlBY2Nlc3NDYWxsRXhwcmVzc2lvbiA9IHRzLkNhbGxFeHByZXNzaW9uICYge2V4cHJlc3Npb246IHRzLlByb3BlcnR5QWNjZXNzRXhwcmVzc2lvbn07XG5cbi8qKiBSZXBsYWNlcyBhbiBpbXBvcnQgaW5zaWRlIGFuIGltcG9ydCBzdGF0ZW1lbnQgd2l0aCBhIGRpZmZlcmVudCBvbmUuICovXG5leHBvcnQgZnVuY3Rpb24gcmVwbGFjZUltcG9ydChub2RlOiB0cy5OYW1lZEltcG9ydHMsIG9sZEltcG9ydDogc3RyaW5nLCBuZXdJbXBvcnQ6IHN0cmluZykge1xuICBjb25zdCBpc0FscmVhZHlJbXBvcnRlZCA9IGZpbmRJbXBvcnRTcGVjaWZpZXIobm9kZS5lbGVtZW50cywgbmV3SW1wb3J0KTtcblxuICBpZiAoaXNBbHJlYWR5SW1wb3J0ZWQpIHtcbiAgICByZXR1cm4gbm9kZTtcbiAgfVxuXG4gIGNvbnN0IGV4aXN0aW5nSW1wb3J0ID0gZmluZEltcG9ydFNwZWNpZmllcihub2RlLmVsZW1lbnRzLCBvbGRJbXBvcnQpO1xuXG4gIGlmICghZXhpc3RpbmdJbXBvcnQpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoYENvdWxkIG5vdCBmaW5kIGFuIGltcG9ydCB0byByZXBsYWNlIHVzaW5nICR7b2xkSW1wb3J0fS5gKTtcbiAgfVxuXG4gIHJldHVybiB0cy51cGRhdGVOYW1lZEltcG9ydHMobm9kZSwgW1xuICAgIC4uLm5vZGUuZWxlbWVudHMuZmlsdGVyKGN1cnJlbnQgPT4gY3VycmVudCAhPT0gZXhpc3RpbmdJbXBvcnQpLFxuICAgIC8vIENyZWF0ZSBhIG5ldyBpbXBvcnQgd2hpbGUgdHJ5aW5nIHRvIHByZXNlcnZlIHRoZSBhbGlhcyBvZiB0aGUgb2xkIG9uZS5cbiAgICB0cy5jcmVhdGVJbXBvcnRTcGVjaWZpZXIoXG4gICAgICAgIGV4aXN0aW5nSW1wb3J0LnByb3BlcnR5TmFtZSA/IHRzLmNyZWF0ZUlkZW50aWZpZXIobmV3SW1wb3J0KSA6IHVuZGVmaW5lZCxcbiAgICAgICAgZXhpc3RpbmdJbXBvcnQucHJvcGVydHlOYW1lID8gZXhpc3RpbmdJbXBvcnQubmFtZSA6IHRzLmNyZWF0ZUlkZW50aWZpZXIobmV3SW1wb3J0KSlcbiAgXSk7XG59XG5cbi8qKlxuICogTWlncmF0ZXMgYSBmdW5jdGlvbiBjYWxsIGV4cHJlc3Npb24gZnJvbSBgUmVuZGVyZXJgIHRvIGBSZW5kZXJlcjJgLlxuICogUmV0dXJucyBudWxsIGlmIHRoZSBleHByZXNzaW9uIHNob3VsZCBiZSBkcm9wcGVkLlxuICovXG5leHBvcnQgZnVuY3Rpb24gbWlncmF0ZUV4cHJlc3Npb24obm9kZTogdHMuQ2FsbEV4cHJlc3Npb24sIHR5cGVDaGVja2VyOiB0cy5UeXBlQ2hlY2tlcik6XG4gICAge25vZGU6IHRzLk5vZGUgfCBudWxsLCByZXF1aXJlZEhlbHBlcnM/OiBIZWxwZXJGdW5jdGlvbltdfSB7XG4gIGlmIChpc1Byb3BlcnR5QWNjZXNzQ2FsbEV4cHJlc3Npb24obm9kZSkpIHtcbiAgICBzd2l0Y2ggKG5vZGUuZXhwcmVzc2lvbi5uYW1lLmdldFRleHQoKSkge1xuICAgICAgY2FzZSAnc2V0RWxlbWVudFByb3BlcnR5JzpcbiAgICAgICAgcmV0dXJuIHtub2RlOiByZW5hbWVNZXRob2RDYWxsKG5vZGUsICdzZXRQcm9wZXJ0eScpfTtcbiAgICAgIGNhc2UgJ3NldFRleHQnOlxuICAgICAgICByZXR1cm4ge25vZGU6IHJlbmFtZU1ldGhvZENhbGwobm9kZSwgJ3NldFZhbHVlJyl9O1xuICAgICAgY2FzZSAnbGlzdGVuR2xvYmFsJzpcbiAgICAgICAgcmV0dXJuIHtub2RlOiByZW5hbWVNZXRob2RDYWxsKG5vZGUsICdsaXN0ZW4nKX07XG4gICAgICBjYXNlICdzZWxlY3RSb290RWxlbWVudCc6XG4gICAgICAgIHJldHVybiB7bm9kZTogbWlncmF0ZVNlbGVjdFJvb3RFbGVtZW50KG5vZGUpfTtcbiAgICAgIGNhc2UgJ3NldEVsZW1lbnRDbGFzcyc6XG4gICAgICAgIHJldHVybiB7bm9kZTogbWlncmF0ZVNldEVsZW1lbnRDbGFzcyhub2RlKX07XG4gICAgICBjYXNlICdzZXRFbGVtZW50U3R5bGUnOlxuICAgICAgICByZXR1cm4ge25vZGU6IG1pZ3JhdGVTZXRFbGVtZW50U3R5bGUobm9kZSwgdHlwZUNoZWNrZXIpfTtcbiAgICAgIGNhc2UgJ2ludm9rZUVsZW1lbnRNZXRob2QnOlxuICAgICAgICByZXR1cm4ge25vZGU6IG1pZ3JhdGVJbnZva2VFbGVtZW50TWV0aG9kKG5vZGUpfTtcbiAgICAgIGNhc2UgJ3NldEJpbmRpbmdEZWJ1Z0luZm8nOlxuICAgICAgICByZXR1cm4ge25vZGU6IG51bGx9O1xuICAgICAgY2FzZSAnY3JlYXRlVmlld1Jvb3QnOlxuICAgICAgICByZXR1cm4ge25vZGU6IG1pZ3JhdGVDcmVhdGVWaWV3Um9vdChub2RlKX07XG4gICAgICBjYXNlICdzZXRFbGVtZW50QXR0cmlidXRlJzpcbiAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICBub2RlOiBzd2l0Y2hUb0hlbHBlckNhbGwobm9kZSwgSGVscGVyRnVuY3Rpb24uc2V0RWxlbWVudEF0dHJpYnV0ZSwgbm9kZS5hcmd1bWVudHMpLFxuICAgICAgICAgIHJlcXVpcmVkSGVscGVyczogW1xuICAgICAgICAgICAgSGVscGVyRnVuY3Rpb24uYW55LCBIZWxwZXJGdW5jdGlvbi5zcGxpdE5hbWVzcGFjZSwgSGVscGVyRnVuY3Rpb24uc2V0RWxlbWVudEF0dHJpYnV0ZVxuICAgICAgICAgIF1cbiAgICAgICAgfTtcbiAgICAgIGNhc2UgJ2NyZWF0ZUVsZW1lbnQnOlxuICAgICAgICByZXR1cm4ge1xuICAgICAgICAgIG5vZGU6IHN3aXRjaFRvSGVscGVyQ2FsbChub2RlLCBIZWxwZXJGdW5jdGlvbi5jcmVhdGVFbGVtZW50LCBub2RlLmFyZ3VtZW50cy5zbGljZSgwLCAyKSksXG4gICAgICAgICAgcmVxdWlyZWRIZWxwZXJzOlxuICAgICAgICAgICAgICBbSGVscGVyRnVuY3Rpb24uYW55LCBIZWxwZXJGdW5jdGlvbi5zcGxpdE5hbWVzcGFjZSwgSGVscGVyRnVuY3Rpb24uY3JlYXRlRWxlbWVudF1cbiAgICAgICAgfTtcbiAgICAgIGNhc2UgJ2NyZWF0ZVRleHQnOlxuICAgICAgICByZXR1cm4ge1xuICAgICAgICAgIG5vZGU6IHN3aXRjaFRvSGVscGVyQ2FsbChub2RlLCBIZWxwZXJGdW5jdGlvbi5jcmVhdGVUZXh0LCBub2RlLmFyZ3VtZW50cy5zbGljZSgwLCAyKSksXG4gICAgICAgICAgcmVxdWlyZWRIZWxwZXJzOiBbSGVscGVyRnVuY3Rpb24uYW55LCBIZWxwZXJGdW5jdGlvbi5jcmVhdGVUZXh0XVxuICAgICAgICB9O1xuICAgICAgY2FzZSAnY3JlYXRlVGVtcGxhdGVBbmNob3InOlxuICAgICAgICByZXR1cm4ge1xuICAgICAgICAgIG5vZGU6IHN3aXRjaFRvSGVscGVyQ2FsbChcbiAgICAgICAgICAgICAgbm9kZSwgSGVscGVyRnVuY3Rpb24uY3JlYXRlVGVtcGxhdGVBbmNob3IsIG5vZGUuYXJndW1lbnRzLnNsaWNlKDAsIDEpKSxcbiAgICAgICAgICByZXF1aXJlZEhlbHBlcnM6IFtIZWxwZXJGdW5jdGlvbi5hbnksIEhlbHBlckZ1bmN0aW9uLmNyZWF0ZVRlbXBsYXRlQW5jaG9yXVxuICAgICAgICB9O1xuICAgICAgY2FzZSAncHJvamVjdE5vZGVzJzpcbiAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICBub2RlOiBzd2l0Y2hUb0hlbHBlckNhbGwobm9kZSwgSGVscGVyRnVuY3Rpb24ucHJvamVjdE5vZGVzLCBub2RlLmFyZ3VtZW50cyksXG4gICAgICAgICAgcmVxdWlyZWRIZWxwZXJzOiBbSGVscGVyRnVuY3Rpb24uYW55LCBIZWxwZXJGdW5jdGlvbi5wcm9qZWN0Tm9kZXNdXG4gICAgICAgIH07XG4gICAgICBjYXNlICdhbmltYXRlJzpcbiAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICBub2RlOiBtaWdyYXRlQW5pbWF0ZUNhbGwoKSxcbiAgICAgICAgICByZXF1aXJlZEhlbHBlcnM6IFtIZWxwZXJGdW5jdGlvbi5hbnksIEhlbHBlckZ1bmN0aW9uLmFuaW1hdGVdXG4gICAgICAgIH07XG4gICAgICBjYXNlICdkZXN0cm95Vmlldyc6XG4gICAgICAgIHJldHVybiB7XG4gICAgICAgICAgbm9kZTogc3dpdGNoVG9IZWxwZXJDYWxsKG5vZGUsIEhlbHBlckZ1bmN0aW9uLmRlc3Ryb3lWaWV3LCBbbm9kZS5hcmd1bWVudHNbMV1dKSxcbiAgICAgICAgICByZXF1aXJlZEhlbHBlcnM6IFtIZWxwZXJGdW5jdGlvbi5hbnksIEhlbHBlckZ1bmN0aW9uLmRlc3Ryb3lWaWV3XVxuICAgICAgICB9O1xuICAgICAgY2FzZSAnZGV0YWNoVmlldyc6XG4gICAgICAgIHJldHVybiB7XG4gICAgICAgICAgbm9kZTogc3dpdGNoVG9IZWxwZXJDYWxsKG5vZGUsIEhlbHBlckZ1bmN0aW9uLmRldGFjaFZpZXcsIFtub2RlLmFyZ3VtZW50c1swXV0pLFxuICAgICAgICAgIHJlcXVpcmVkSGVscGVyczogW0hlbHBlckZ1bmN0aW9uLmFueSwgSGVscGVyRnVuY3Rpb24uZGV0YWNoVmlld11cbiAgICAgICAgfTtcbiAgICAgIGNhc2UgJ2F0dGFjaFZpZXdBZnRlcic6XG4gICAgICAgIHJldHVybiB7XG4gICAgICAgICAgbm9kZTogc3dpdGNoVG9IZWxwZXJDYWxsKG5vZGUsIEhlbHBlckZ1bmN0aW9uLmF0dGFjaFZpZXdBZnRlciwgbm9kZS5hcmd1bWVudHMpLFxuICAgICAgICAgIHJlcXVpcmVkSGVscGVyczogW0hlbHBlckZ1bmN0aW9uLmFueSwgSGVscGVyRnVuY3Rpb24uYXR0YWNoVmlld0FmdGVyXVxuICAgICAgICB9O1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiB7bm9kZX07XG59XG5cbi8qKiBDaGVja3Mgd2hldGhlciBhIG5vZGUgaXMgYSBQcm9wZXJ0eUFjY2Vzc0V4cHJlc3Npb24uICovXG5mdW5jdGlvbiBpc1Byb3BlcnR5QWNjZXNzQ2FsbEV4cHJlc3Npb24obm9kZTogdHMuTm9kZSk6IG5vZGUgaXMgUHJvcGVydHlBY2Nlc3NDYWxsRXhwcmVzc2lvbiB7XG4gIHJldHVybiB0cy5pc0NhbGxFeHByZXNzaW9uKG5vZGUpICYmIHRzLmlzUHJvcGVydHlBY2Nlc3NFeHByZXNzaW9uKG5vZGUuZXhwcmVzc2lvbik7XG59XG5cbi8qKiBSZW5hbWVzIGEgbWV0aG9kIGNhbGwgd2hpbGUga2VlcGluZyBhbGwgb2YgdGhlIHBhcmFtZXRlcnMgaW4gcGxhY2UuICovXG5mdW5jdGlvbiByZW5hbWVNZXRob2RDYWxsKG5vZGU6IFByb3BlcnR5QWNjZXNzQ2FsbEV4cHJlc3Npb24sIG5ld05hbWU6IHN0cmluZyk6IHRzLkNhbGxFeHByZXNzaW9uIHtcbiAgY29uc3QgbmV3RXhwcmVzc2lvbiA9IHRzLnVwZGF0ZVByb3BlcnR5QWNjZXNzKFxuICAgICAgbm9kZS5leHByZXNzaW9uLCBub2RlLmV4cHJlc3Npb24uZXhwcmVzc2lvbiwgdHMuY3JlYXRlSWRlbnRpZmllcihuZXdOYW1lKSk7XG5cbiAgcmV0dXJuIHRzLnVwZGF0ZUNhbGwobm9kZSwgbmV3RXhwcmVzc2lvbiwgbm9kZS50eXBlQXJndW1lbnRzLCBub2RlLmFyZ3VtZW50cyk7XG59XG5cbi8qKlxuICogTWlncmF0ZXMgYSBgc2VsZWN0Um9vdEVsZW1lbnRgIGNhbGwgYnkgcmVtb3ZpbmcgdGhlIGxhc3QgYXJndW1lbnQgd2hpY2ggaXMgbm8gbG9uZ2VyIHN1cHBvcnRlZC5cbiAqL1xuZnVuY3Rpb24gbWlncmF0ZVNlbGVjdFJvb3RFbGVtZW50KG5vZGU6IHRzLkNhbGxFeHByZXNzaW9uKTogdHMuTm9kZSB7XG4gIC8vIFRoZSBvbmx5IHRoaW5nIHdlIG5lZWQgdG8gZG8gaXMgdG8gZHJvcCB0aGUgbGFzdCBhcmd1bWVudFxuICAvLyAoYGRlYnVnSW5mb2ApLCBpZiB0aGUgY29uc3VtZXIgd2FzIHBhc3NpbmcgaXQgaW4uXG4gIGlmIChub2RlLmFyZ3VtZW50cy5sZW5ndGggPiAxKSB7XG4gICAgcmV0dXJuIHRzLnVwZGF0ZUNhbGwobm9kZSwgbm9kZS5leHByZXNzaW9uLCBub2RlLnR5cGVBcmd1bWVudHMsIFtub2RlLmFyZ3VtZW50c1swXV0pO1xuICB9XG5cbiAgcmV0dXJuIG5vZGU7XG59XG5cbi8qKlxuICogTWlncmF0ZXMgYSBjYWxsIHRvIGBzZXRFbGVtZW50Q2xhc3NgIGVpdGhlciB0byBhIGNhbGwgdG8gYGFkZENsYXNzYCBvciBgcmVtb3ZlQ2xhc3NgLCBvclxuICogdG8gYW4gZXhwcmVzc2lvbiBsaWtlIGBpc0FkZCA/IGFkZENsYXNzKGVsLCBjbGFzc05hbWUpIDogcmVtb3ZlQ2xhc3MoZWwsIGNsYXNzTmFtZSlgLlxuICovXG5mdW5jdGlvbiBtaWdyYXRlU2V0RWxlbWVudENsYXNzKG5vZGU6IFByb3BlcnR5QWNjZXNzQ2FsbEV4cHJlc3Npb24pOiB0cy5Ob2RlIHtcbiAgLy8gQ2xvbmUgc28gd2UgZG9uJ3QgbXV0YXRlIGJ5IGFjY2lkZW50LiBOb3RlIHRoYXQgd2UgYXNzdW1lIHRoYXRcbiAgLy8gdGhlIHVzZXIncyBjb2RlIGlzIHByb3ZpZGluZyBhbGwgdGhyZWUgcmVxdWlyZWQgYXJndW1lbnRzLlxuICBjb25zdCBvdXRwdXRNZXRob2RBcmdzID0gbm9kZS5hcmd1bWVudHMuc2xpY2UoKTtcbiAgY29uc3QgaXNBZGRBcmd1bWVudCA9IG91dHB1dE1ldGhvZEFyZ3MucG9wKCkgITtcbiAgY29uc3QgY3JlYXRlUmVuZGVyZXJDYWxsID0gKGlzQWRkOiBib29sZWFuKSA9PiB7XG4gICAgY29uc3QgaW5uZXJFeHByZXNzaW9uID0gbm9kZS5leHByZXNzaW9uLmV4cHJlc3Npb247XG4gICAgY29uc3QgdG9wRXhwcmVzc2lvbiA9XG4gICAgICAgIHRzLmNyZWF0ZVByb3BlcnR5QWNjZXNzKGlubmVyRXhwcmVzc2lvbiwgaXNBZGQgPyAnYWRkQ2xhc3MnIDogJ3JlbW92ZUNsYXNzJyk7XG4gICAgcmV0dXJuIHRzLmNyZWF0ZUNhbGwodG9wRXhwcmVzc2lvbiwgW10sIG5vZGUuYXJndW1lbnRzLnNsaWNlKDAsIDIpKTtcbiAgfTtcblxuICAvLyBJZiB0aGUgY2FsbCBoYXMgdGhlIGBpc0FkZGAgYXJndW1lbnQgYXMgYSBsaXRlcmFsIGJvb2xlYW4sIHdlIGNhbiBtYXAgaXQgZGlyZWN0bHkgdG9cbiAgLy8gYGFkZENsYXNzYCBvciBgcmVtb3ZlQ2xhc3NgLiBOb3RlIHRoYXQgd2UgY2FuJ3QgdXNlIHRoZSB0eXBlIGNoZWNrZXIgaGVyZSwgYmVjYXVzZSBpdFxuICAvLyB3b24ndCB0ZWxsIHVzIHdoZXRoZXIgdGhlIHZhbHVlIHJlc29sdmVzIHRvIHRydWUgb3IgZmFsc2UuXG4gIGlmIChpc0FkZEFyZ3VtZW50LmtpbmQgPT09IHRzLlN5bnRheEtpbmQuVHJ1ZUtleXdvcmQgfHxcbiAgICAgIGlzQWRkQXJndW1lbnQua2luZCA9PT0gdHMuU3ludGF4S2luZC5GYWxzZUtleXdvcmQpIHtcbiAgICByZXR1cm4gY3JlYXRlUmVuZGVyZXJDYWxsKGlzQWRkQXJndW1lbnQua2luZCA9PT0gdHMuU3ludGF4S2luZC5UcnVlS2V5d29yZCk7XG4gIH1cblxuICAvLyBPdGhlcndpc2UgY3JlYXRlIGEgdGVybmFyeSBvbiB0aGUgdmFyaWFibGUuXG4gIHJldHVybiB0cy5jcmVhdGVDb25kaXRpb25hbChpc0FkZEFyZ3VtZW50LCBjcmVhdGVSZW5kZXJlckNhbGwodHJ1ZSksIGNyZWF0ZVJlbmRlcmVyQ2FsbChmYWxzZSkpO1xufVxuXG4vKipcbiAqIE1pZ3JhdGVzIGEgY2FsbCB0byBgc2V0RWxlbWVudFN0eWxlYCBjYWxsIGVpdGhlciB0byBhIGNhbGwgdG9cbiAqIGBzZXRTdHlsZWAgb3IgYHJlbW92ZVN0eWxlYC4gb3IgdG8gYW4gZXhwcmVzc2lvbiBsaWtlXG4gKiBgdmFsdWUgPT0gbnVsbCA/IHJlbW92ZVN0eWxlKGVsLCBrZXkpIDogc2V0U3R5bGUoZWwsIGtleSwgdmFsdWUpYC5cbiAqL1xuZnVuY3Rpb24gbWlncmF0ZVNldEVsZW1lbnRTdHlsZShcbiAgICBub2RlOiBQcm9wZXJ0eUFjY2Vzc0NhbGxFeHByZXNzaW9uLCB0eXBlQ2hlY2tlcjogdHMuVHlwZUNoZWNrZXIpOiB0cy5Ob2RlIHtcbiAgY29uc3QgYXJncyA9IG5vZGUuYXJndW1lbnRzO1xuICBjb25zdCBhZGRNZXRob2ROYW1lID0gJ3NldFN0eWxlJztcbiAgY29uc3QgcmVtb3ZlTWV0aG9kTmFtZSA9ICdyZW1vdmVTdHlsZSc7XG4gIGNvbnN0IGxhc3RBcmdUeXBlID0gYXJnc1syXSA/XG4gICAgICB0eXBlQ2hlY2tlci50eXBlVG9TdHJpbmcoXG4gICAgICAgICAgdHlwZUNoZWNrZXIuZ2V0VHlwZUF0TG9jYXRpb24oYXJnc1syXSksIG5vZGUsIHRzLlR5cGVGb3JtYXRGbGFncy5BZGRVbmRlZmluZWQpIDpcbiAgICAgIG51bGw7XG5cbiAgLy8gTm90ZSB0aGF0IGZvciBhIGxpdGVyYWwgbnVsbCwgVFMgY29uc2lkZXJzIGl0IGEgYE51bGxLZXl3b3JkYCxcbiAgLy8gd2hlcmVhcyBhIGxpdGVyYWwgYHVuZGVmaW5lZGAgaXMganVzdCBhbiBJZGVudGlmaWVyLlxuICBpZiAoYXJncy5sZW5ndGggPT09IDIgfHwgbGFzdEFyZ1R5cGUgPT09ICdudWxsJyB8fCBsYXN0QXJnVHlwZSA9PT0gJ3VuZGVmaW5lZCcpIHtcbiAgICAvLyBJZiB3ZSd2ZSBnb3QgYSBjYWxsIHdpdGggdHdvIGFyZ3VtZW50cywgb3Igb25lIHdpdGggdGhyZWUgYXJndW1lbnRzIHdoZXJlIHRoZSBsYXN0IG9uZSBpc1xuICAgIC8vIGB1bmRlZmluZWRgIG9yIGBudWxsYCwgd2UgY2FuIHNhZmVseSBzd2l0Y2ggdG8gYSBgcmVtb3ZlU3R5bGVgIGNhbGwuXG4gICAgY29uc3QgaW5uZXJFeHByZXNzaW9uID0gbm9kZS5leHByZXNzaW9uLmV4cHJlc3Npb247XG4gICAgY29uc3QgdG9wRXhwcmVzc2lvbiA9IHRzLmNyZWF0ZVByb3BlcnR5QWNjZXNzKGlubmVyRXhwcmVzc2lvbiwgcmVtb3ZlTWV0aG9kTmFtZSk7XG4gICAgcmV0dXJuIHRzLmNyZWF0ZUNhbGwodG9wRXhwcmVzc2lvbiwgW10sIGFyZ3Muc2xpY2UoMCwgMikpO1xuICB9IGVsc2UgaWYgKGFyZ3MubGVuZ3RoID09PSAzKSB7XG4gICAgLy8gV2UgbmVlZCB0aGUgY2hlY2tzIGZvciBzdHJpbmcgbGl0ZXJhbHMsIGJlY2F1c2UgdGhlIHR5cGUgb2Ygc29tZXRoaW5nXG4gICAgLy8gbGlrZSBgXCJibHVlXCJgIGlzIHRoZSBsaXRlcmFsIGBibHVlYCwgbm90IGBzdHJpbmdgLlxuICAgIGlmIChsYXN0QXJnVHlwZSA9PT0gJ3N0cmluZycgfHwgbGFzdEFyZ1R5cGUgPT09ICdudW1iZXInIHx8IHRzLmlzU3RyaW5nTGl0ZXJhbChhcmdzWzJdKSB8fFxuICAgICAgICB0cy5pc05vU3Vic3RpdHV0aW9uVGVtcGxhdGVMaXRlcmFsKGFyZ3NbMl0pIHx8IHRzLmlzTnVtZXJpY0xpdGVyYWwoYXJnc1syXSkpIHtcbiAgICAgIC8vIElmIHdlJ3ZlIGdvdCB0aHJlZSBhcmd1bWVudHMgYW5kIHRoZSBsYXN0IG9uZSBpcyBhIHN0cmluZyBsaXRlcmFsIG9yIGEgbnVtYmVyLCB3ZVxuICAgICAgLy8gY2FuIHNhZmVseSByZW5hbWUgdG8gYHNldFN0eWxlYC5cbiAgICAgIHJldHVybiByZW5hbWVNZXRob2RDYWxsKG5vZGUsIGFkZE1ldGhvZE5hbWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICAvLyBPdGhlcndpc2UgbWlncmF0ZSB0byBhIHRlcm5hcnkgdGhhdCBsb29rcyBsaWtlOlxuICAgICAgLy8gYHZhbHVlID09IG51bGwgPyByZW1vdmVTdHlsZShlbCwga2V5KSA6IHNldFN0eWxlKGVsLCBrZXksIHZhbHVlKWBcbiAgICAgIGNvbnN0IGNvbmRpdGlvbiA9IHRzLmNyZWF0ZUJpbmFyeShhcmdzWzJdLCB0cy5TeW50YXhLaW5kLkVxdWFsc0VxdWFsc1Rva2VuLCB0cy5jcmVhdGVOdWxsKCkpO1xuICAgICAgY29uc3Qgd2hlbk51bGxDYWxsID0gcmVuYW1lTWV0aG9kQ2FsbChcbiAgICAgICAgICB0cy5jcmVhdGVDYWxsKG5vZGUuZXhwcmVzc2lvbiwgW10sIGFyZ3Muc2xpY2UoMCwgMikpIGFzIFByb3BlcnR5QWNjZXNzQ2FsbEV4cHJlc3Npb24sXG4gICAgICAgICAgcmVtb3ZlTWV0aG9kTmFtZSk7XG4gICAgICByZXR1cm4gdHMuY3JlYXRlQ29uZGl0aW9uYWwoY29uZGl0aW9uLCB3aGVuTnVsbENhbGwsIHJlbmFtZU1ldGhvZENhbGwobm9kZSwgYWRkTWV0aG9kTmFtZSkpO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBub2RlO1xufVxuXG4vKipcbiAqIE1pZ3JhdGVzIGEgY2FsbCB0byBgaW52b2tlRWxlbWVudE1ldGhvZCh0YXJnZXQsIG1ldGhvZCwgW2FyZzEsIGFyZzJdKWAgZWl0aGVyIHRvXG4gKiBgdGFyZ2V0Lm1ldGhvZChhcmcxLCBhcmcyKWAgb3IgYCh0YXJnZXQgYXMgYW55KVttZXRob2RdLmFwcGx5KHRhcmdldCwgW2FyZzEsIGFyZzJdKWAuXG4gKi9cbmZ1bmN0aW9uIG1pZ3JhdGVJbnZva2VFbGVtZW50TWV0aG9kKG5vZGU6IHRzLkNhbGxFeHByZXNzaW9uKTogdHMuTm9kZSB7XG4gIGNvbnN0IFt0YXJnZXQsIG5hbWUsIGFyZ3NdID0gbm9kZS5hcmd1bWVudHM7XG4gIGNvbnN0IGlzTmFtZVN0YXRpYyA9IHRzLmlzU3RyaW5nTGl0ZXJhbChuYW1lKSB8fCB0cy5pc05vU3Vic3RpdHV0aW9uVGVtcGxhdGVMaXRlcmFsKG5hbWUpO1xuICBjb25zdCBpc0FyZ3NTdGF0aWMgPSAhYXJncyB8fCB0cy5pc0FycmF5TGl0ZXJhbEV4cHJlc3Npb24oYXJncyk7XG5cbiAgaWYgKGlzTmFtZVN0YXRpYyAmJiBpc0FyZ3NTdGF0aWMpIHtcbiAgICAvLyBJZiB0aGUgbmFtZSBpcyBhIHN0YXRpYyBzdHJpbmcgYW5kIHRoZSBhcmd1bWVudHMgYXJlIGFuIGFycmF5IGxpdGVyYWwsXG4gICAgLy8gd2UgY2FuIHNhZmVseSBjb252ZXJ0IHRoZSBub2RlIGludG8gYSBjYWxsIGV4cHJlc3Npb24uXG4gICAgY29uc3QgZXhwcmVzc2lvbiA9IHRzLmNyZWF0ZVByb3BlcnR5QWNjZXNzKFxuICAgICAgICB0YXJnZXQsIChuYW1lIGFzIHRzLlN0cmluZ0xpdGVyYWwgfCB0cy5Ob1N1YnN0aXR1dGlvblRlbXBsYXRlTGl0ZXJhbCkudGV4dCk7XG4gICAgY29uc3QgY2FsbEFyZ3VtZW50cyA9IGFyZ3MgPyAoYXJncyBhcyB0cy5BcnJheUxpdGVyYWxFeHByZXNzaW9uKS5lbGVtZW50cyA6IFtdO1xuICAgIHJldHVybiB0cy5jcmVhdGVDYWxsKGV4cHJlc3Npb24sIFtdLCBjYWxsQXJndW1lbnRzKTtcbiAgfSBlbHNlIHtcbiAgICAvLyBPdGhlcndpc2UgY3JlYXRlIGFuIGV4cHJlc3Npb24gaW4gdGhlIGZvcm0gb2YgYCh0YXJnZXQgYXMgYW55KVtuYW1lXS5hcHBseSh0YXJnZXQsIGFyZ3MpYC5cbiAgICBjb25zdCBhc0V4cHJlc3Npb24gPSB0cy5jcmVhdGVQYXJlbihcbiAgICAgICAgdHMuY3JlYXRlQXNFeHByZXNzaW9uKHRhcmdldCwgdHMuY3JlYXRlS2V5d29yZFR5cGVOb2RlKHRzLlN5bnRheEtpbmQuQW55S2V5d29yZCkpKTtcbiAgICBjb25zdCBlbGVtZW50QWNjZXNzID0gdHMuY3JlYXRlRWxlbWVudEFjY2Vzcyhhc0V4cHJlc3Npb24sIG5hbWUpO1xuICAgIGNvbnN0IGFwcGx5RXhwcmVzc2lvbiA9IHRzLmNyZWF0ZVByb3BlcnR5QWNjZXNzKGVsZW1lbnRBY2Nlc3MsICdhcHBseScpO1xuICAgIHJldHVybiB0cy5jcmVhdGVDYWxsKGFwcGx5RXhwcmVzc2lvbiwgW10sIGFyZ3MgPyBbdGFyZ2V0LCBhcmdzXSA6IFt0YXJnZXRdKTtcbiAgfVxufVxuXG4vKiogTWlncmF0ZXMgYSBjYWxsIHRvIGBjcmVhdGVWaWV3Um9vdGAgdG8gd2hhdGV2ZXIgbm9kZSB3YXMgcGFzc2VkIGluIGFzIHRoZSBmaXJzdCBhcmd1bWVudC4gKi9cbmZ1bmN0aW9uIG1pZ3JhdGVDcmVhdGVWaWV3Um9vdChub2RlOiB0cy5DYWxsRXhwcmVzc2lvbik6IHRzLk5vZGUge1xuICByZXR1cm4gbm9kZS5hcmd1bWVudHNbMF07XG59XG5cbi8qKiBNaWdyYXRlcyBhIGNhbGwgdG8gYG1pZ3JhdGVgIGEgZGlyZWN0IGNhbGwgdG8gdGhlIGhlbHBlci4gKi9cbmZ1bmN0aW9uIG1pZ3JhdGVBbmltYXRlQ2FsbCgpIHtcbiAgcmV0dXJuIHRzLmNyZWF0ZUNhbGwodHMuY3JlYXRlSWRlbnRpZmllcihIZWxwZXJGdW5jdGlvbi5hbmltYXRlKSwgW10sIFtdKTtcbn1cblxuLyoqXG4gKiBTd2l0Y2hlcyBvdXQgYSBjYWxsIHRvIHRoZSBgUmVuZGVyZXJgIHRvIGEgY2FsbCB0byBvbmUgb2Ygb3VyIGhlbHBlciBmdW5jdGlvbnMuXG4gKiBNb3N0IG9mIHRoZSBoZWxwZXJzIGFjY2VwdCBhbiBpbnN0YW5jZSBvZiBgUmVuZGVyZXIyYCBhcyB0aGUgZmlyc3QgYXJndW1lbnQgYW5kIGFsbFxuICogc3Vic2VxdWVudCBhcmd1bWVudHMgZGlmZmVyLlxuICogQHBhcmFtIG5vZGUgTm9kZSBvZiB0aGUgb3JpZ2luYWwgbWV0aG9kIGNhbGwuXG4gKiBAcGFyYW0gaGVscGVyIE5hbWUgb2YgdGhlIGhlbHBlciB3aXRoIHdoaWNoIHRvIHJlcGxhY2UgdGhlIG9yaWdpbmFsIGNhbGwuXG4gKiBAcGFyYW0gYXJncyBBcmd1bWVudHMgdGhhdCBzaG91bGQgYmUgcGFzc2VkIGludG8gdGhlIGhlbHBlciBhZnRlciB0aGUgcmVuZGVyZXIgYXJndW1lbnQuXG4gKi9cbmZ1bmN0aW9uIHN3aXRjaFRvSGVscGVyQ2FsbChcbiAgICBub2RlOiBQcm9wZXJ0eUFjY2Vzc0NhbGxFeHByZXNzaW9uLCBoZWxwZXI6IEhlbHBlckZ1bmN0aW9uLFxuICAgIGFyZ3M6IHRzLkV4cHJlc3Npb25bXSB8IHRzLk5vZGVBcnJheTx0cy5FeHByZXNzaW9uPik6IHRzLk5vZGUge1xuICByZXR1cm4gdHMuY3JlYXRlQ2FsbCh0cy5jcmVhdGVJZGVudGlmaWVyKGhlbHBlciksIFtdLCBbbm9kZS5leHByZXNzaW9uLmV4cHJlc3Npb24sIC4uLmFyZ3NdKTtcbn1cbiJdfQ==