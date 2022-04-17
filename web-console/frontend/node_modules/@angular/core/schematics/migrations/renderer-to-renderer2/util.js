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
        define("@angular/core/schematics/migrations/renderer-to-renderer2/util", ["require", "exports", "typescript"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const ts = require("typescript");
    /**
     * Finds typed nodes (e.g. function parameters or class properties) that are referencing the old
     * `Renderer`, as well as calls to the `Renderer` methods.
     */
    function findRendererReferences(sourceFile, typeChecker, rendererImport) {
        const typedNodes = new Set();
        const methodCalls = new Set();
        const forwardRefs = new Set();
        const importSpecifier = findImportSpecifier(rendererImport.elements, 'Renderer');
        const forwardRefImport = findCoreImport(sourceFile, 'forwardRef');
        const forwardRefSpecifier = forwardRefImport ? findImportSpecifier(forwardRefImport.elements, 'forwardRef') : null;
        ts.forEachChild(sourceFile, function visitNode(node) {
            if ((ts.isParameter(node) || ts.isPropertyDeclaration(node)) &&
                isReferenceToImport(typeChecker, node.name, importSpecifier)) {
                typedNodes.add(node);
            }
            else if (ts.isAsExpression(node) && isReferenceToImport(typeChecker, node.type, importSpecifier)) {
                typedNodes.add(node);
            }
            else if (ts.isCallExpression(node)) {
                if (ts.isPropertyAccessExpression(node.expression) &&
                    isReferenceToImport(typeChecker, node.expression.expression, importSpecifier)) {
                    methodCalls.add(node);
                }
                else if (
                // If we're dealing with a forwardRef that's returning a Renderer.
                forwardRefSpecifier && ts.isIdentifier(node.expression) &&
                    isReferenceToImport(typeChecker, node.expression, forwardRefSpecifier) &&
                    node.arguments.length) {
                    const rendererIdentifier = findRendererIdentifierInForwardRef(typeChecker, node, importSpecifier);
                    if (rendererIdentifier) {
                        forwardRefs.add(rendererIdentifier);
                    }
                }
            }
            ts.forEachChild(node, visitNode);
        });
        return { typedNodes, methodCalls, forwardRefs };
    }
    exports.findRendererReferences = findRendererReferences;
    /** Finds the import from @angular/core that has a symbol with a particular name. */
    function findCoreImport(sourceFile, symbolName) {
        // Only look through the top-level imports.
        for (const node of sourceFile.statements) {
            if (!ts.isImportDeclaration(node) || !ts.isStringLiteral(node.moduleSpecifier) ||
                node.moduleSpecifier.text !== '@angular/core') {
                continue;
            }
            const namedBindings = node.importClause && node.importClause.namedBindings;
            if (!namedBindings || !ts.isNamedImports(namedBindings)) {
                continue;
            }
            if (findImportSpecifier(namedBindings.elements, symbolName)) {
                return namedBindings;
            }
        }
        return null;
    }
    exports.findCoreImport = findCoreImport;
    /** Finds an import specifier with a particular name, accounting for aliases. */
    function findImportSpecifier(elements, importName) {
        return elements.find(element => {
            const { name, propertyName } = element;
            return propertyName ? propertyName.text === importName : name.text === importName;
        }) ||
            null;
    }
    exports.findImportSpecifier = findImportSpecifier;
    /** Checks whether a node is referring to an import spcifier. */
    function isReferenceToImport(typeChecker, node, importSpecifier) {
        if (importSpecifier) {
            const nodeSymbol = typeChecker.getTypeAtLocation(node).getSymbol();
            const importSymbol = typeChecker.getTypeAtLocation(importSpecifier).getSymbol();
            return !!(nodeSymbol && importSymbol) &&
                nodeSymbol.valueDeclaration === importSymbol.valueDeclaration;
        }
        return false;
    }
    /** Finds the identifier referring to the `Renderer` inside a `forwardRef` call expression. */
    function findRendererIdentifierInForwardRef(typeChecker, node, rendererImport) {
        const firstArg = node.arguments[0];
        if (ts.isArrowFunction(firstArg)) {
            // Check if the function is `forwardRef(() => Renderer)`.
            if (ts.isIdentifier(firstArg.body) &&
                isReferenceToImport(typeChecker, firstArg.body, rendererImport)) {
                return firstArg.body;
            }
            else if (ts.isBlock(firstArg.body) && ts.isReturnStatement(firstArg.body.statements[0])) {
                // Otherwise check if the expression is `forwardRef(() => { return Renderer })`.
                const returnStatement = firstArg.body.statements[0];
                if (returnStatement.expression && ts.isIdentifier(returnStatement.expression) &&
                    isReferenceToImport(typeChecker, returnStatement.expression, rendererImport)) {
                    return returnStatement.expression;
                }
            }
        }
        return null;
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc2NoZW1hdGljcy9taWdyYXRpb25zL3JlbmRlcmVyLXRvLXJlbmRlcmVyMi91dGlsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgsaUNBQWlDO0lBRWpDOzs7T0FHRztJQUNILFNBQWdCLHNCQUFzQixDQUNsQyxVQUF5QixFQUFFLFdBQTJCLEVBQUUsY0FBK0I7UUFDekYsTUFBTSxVQUFVLEdBQUcsSUFBSSxHQUFHLEVBQWtFLENBQUM7UUFDN0YsTUFBTSxXQUFXLEdBQUcsSUFBSSxHQUFHLEVBQXFCLENBQUM7UUFDakQsTUFBTSxXQUFXLEdBQUcsSUFBSSxHQUFHLEVBQWlCLENBQUM7UUFDN0MsTUFBTSxlQUFlLEdBQUcsbUJBQW1CLENBQUMsY0FBYyxDQUFDLFFBQVEsRUFBRSxVQUFVLENBQUMsQ0FBQztRQUNqRixNQUFNLGdCQUFnQixHQUFHLGNBQWMsQ0FBQyxVQUFVLEVBQUUsWUFBWSxDQUFDLENBQUM7UUFDbEUsTUFBTSxtQkFBbUIsR0FDckIsZ0JBQWdCLENBQUMsQ0FBQyxDQUFDLG1CQUFtQixDQUFDLGdCQUFnQixDQUFDLFFBQVEsRUFBRSxZQUFZLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO1FBRTNGLEVBQUUsQ0FBQyxZQUFZLENBQUMsVUFBVSxFQUFFLFNBQVMsU0FBUyxDQUFDLElBQWE7WUFDMUQsSUFBSSxDQUFDLEVBQUUsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLElBQUksRUFBRSxDQUFDLHFCQUFxQixDQUFDLElBQUksQ0FBQyxDQUFDO2dCQUN4RCxtQkFBbUIsQ0FBQyxXQUFXLEVBQUUsSUFBSSxDQUFDLElBQUksRUFBRSxlQUFlLENBQUMsRUFBRTtnQkFDaEUsVUFBVSxDQUFDLEdBQUcsQ0FBQyxJQUFJLENBQUMsQ0FBQzthQUN0Qjtp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RSxtQkFBbUIsQ0FBQyxXQUFXLEVBQUUsZUFBZSxDQUFDLFVBQVUsRUFBRSxjQUFjLENBQUMsRUFBRTtvQkFDaEYsT0FBTyxlQUFlLENBQUMsVUFBVSxDQUFDO2lCQUNuQzthQUNGO1NBQ0Y7UUFFRCxPQUFPLElBQUksQ0FBQztJQUNkLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCAqIGFzIHRzIGZyb20gJ3R5cGVzY3JpcHQnO1xuXG4vKipcbiAqIEZpbmRzIHR5cGVkIG5vZGVzIChlLmcuIGZ1bmN0aW9uIHBhcmFtZXRlcnMgb3IgY2xhc3MgcHJvcGVydGllcykgdGhhdCBhcmUgcmVmZXJlbmNpbmcgdGhlIG9sZFxuICogYFJlbmRlcmVyYCwgYXMgd2VsbCBhcyBjYWxscyB0byB0aGUgYFJlbmRlcmVyYCBtZXRob2RzLlxuICovXG5leHBvcnQgZnVuY3Rpb24gZmluZFJlbmRlcmVyUmVmZXJlbmNlcyhcbiAgICBzb3VyY2VGaWxlOiB0cy5Tb3VyY2VGaWxlLCB0eXBlQ2hlY2tlcjogdHMuVHlwZUNoZWNrZXIsIHJlbmRlcmVySW1wb3J0OiB0cy5OYW1lZEltcG9ydHMpIHtcbiAgY29uc3QgdHlwZWROb2RlcyA9IG5ldyBTZXQ8dHMuUGFyYW1ldGVyRGVjbGFyYXRpb258dHMuUHJvcGVydHlEZWNsYXJhdGlvbnx0cy5Bc0V4cHJlc3Npb24+KCk7XG4gIGNvbnN0IG1ldGhvZENhbGxzID0gbmV3IFNldDx0cy5DYWxsRXhwcmVzc2lvbj4oKTtcbiAgY29uc3QgZm9yd2FyZFJlZnMgPSBuZXcgU2V0PHRzLklkZW50aWZpZXI+KCk7XG4gIGNvbnN0IGltcG9ydFNwZWNpZmllciA9IGZpbmRJbXBvcnRTcGVjaWZpZXIocmVuZGVyZXJJbXBvcnQuZWxlbWVudHMsICdSZW5kZXJlcicpO1xuICBjb25zdCBmb3J3YXJkUmVmSW1wb3J0ID0gZmluZENvcmVJbXBvcnQoc291cmNlRmlsZSwgJ2ZvcndhcmRSZWYnKTtcbiAgY29uc3QgZm9yd2FyZFJlZlNwZWNpZmllciA9XG4gICAgICBmb3J3YXJkUmVmSW1wb3J0ID8gZmluZEltcG9ydFNwZWNpZmllcihmb3J3YXJkUmVmSW1wb3J0LmVsZW1lbnRzLCAnZm9yd2FyZFJlZicpIDogbnVsbDtcblxuICB0cy5mb3JFYWNoQ2hpbGQoc291cmNlRmlsZSwgZnVuY3Rpb24gdmlzaXROb2RlKG5vZGU6IHRzLk5vZGUpIHtcbiAgICBpZiAoKHRzLmlzUGFyYW1ldGVyKG5vZGUpIHx8IHRzLmlzUHJvcGVydHlEZWNsYXJhdGlvbihub2RlKSkgJiZcbiAgICAgICAgaXNSZWZlcmVuY2VUb0ltcG9ydCh0eXBlQ2hlY2tlciwgbm9kZS5uYW1lLCBpbXBvcnRTcGVjaWZpZXIpKSB7XG4gICAgICB0eXBlZE5vZGVzLmFkZChub2RlKTtcbiAgICB9IGVsc2UgaWYgKFxuICAgICAgICB0cy5pc0FzRXhwcmVzc2lvbihub2RlKSAmJiBpc1JlZmVyZW5jZVRvSW1wb3J0KHR5cGVDaGVja2VyLCBub2RlLnR5cGUsIGltcG9ydFNwZWNpZmllcikpIHtcbiAgICAgIHR5cGVkTm9kZXMuYWRkKG5vZGUpO1xuICAgIH0gZWxzZSBpZiAodHMuaXNDYWxsRXhwcmVzc2lvbihub2RlKSkge1xuICAgICAgaWYgKHRzLmlzUHJvcGVydHlBY2Nlc3NFeHByZXNzaW9uKG5vZGUuZXhwcmVzc2lvbikgJiZcbiAgICAgICAgICBpc1JlZmVyZW5jZVRvSW1wb3J0KHR5cGVDaGVja2VyLCBub2RlLmV4cHJlc3Npb24uZXhwcmVzc2lvbiwgaW1wb3J0U3BlY2lmaWVyKSkge1xuICAgICAgICBtZXRob2RDYWxscy5hZGQobm9kZSk7XG4gICAgICB9IGVsc2UgaWYgKFxuICAgICAgICAgIC8vIElmIHdlJ3JlIGRlYWxpbmcgd2l0aCBhIGZvcndhcmRSZWYgdGhhdCdzIHJldHVybmluZyBhIFJlbmRlcmVyLlxuICAgICAgICAgIGZvcndhcmRSZWZTcGVjaWZpZXIgJiYgdHMuaXNJZGVudGlmaWVyKG5vZGUuZXhwcmVzc2lvbikgJiZcbiAgICAgICAgICBpc1JlZmVyZW5jZVRvSW1wb3J0KHR5cGVDaGVja2VyLCBub2RlLmV4cHJlc3Npb24sIGZvcndhcmRSZWZTcGVjaWZpZXIpICYmXG4gICAgICAgICAgbm9kZS5hcmd1bWVudHMubGVuZ3RoKSB7XG4gICAgICAgIGNvbnN0IHJlbmRlcmVySWRlbnRpZmllciA9XG4gICAgICAgICAgICBmaW5kUmVuZGVyZXJJZGVudGlmaWVySW5Gb3J3YXJkUmVmKHR5cGVDaGVja2VyLCBub2RlLCBpbXBvcnRTcGVjaWZpZXIpO1xuICAgICAgICBpZiAocmVuZGVyZXJJZGVudGlmaWVyKSB7XG4gICAgICAgICAgZm9yd2FyZFJlZnMuYWRkKHJlbmRlcmVySWRlbnRpZmllcik7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICB0cy5mb3JFYWNoQ2hpbGQobm9kZSwgdmlzaXROb2RlKTtcbiAgfSk7XG5cbiAgcmV0dXJuIHt0eXBlZE5vZGVzLCBtZXRob2RDYWxscywgZm9yd2FyZFJlZnN9O1xufVxuXG4vKiogRmluZHMgdGhlIGltcG9ydCBmcm9tIEBhbmd1bGFyL2NvcmUgdGhhdCBoYXMgYSBzeW1ib2wgd2l0aCBhIHBhcnRpY3VsYXIgbmFtZS4gKi9cbmV4cG9ydCBmdW5jdGlvbiBmaW5kQ29yZUltcG9ydChzb3VyY2VGaWxlOiB0cy5Tb3VyY2VGaWxlLCBzeW1ib2xOYW1lOiBzdHJpbmcpOiB0cy5OYW1lZEltcG9ydHN8XG4gICAgbnVsbCB7XG4gIC8vIE9ubHkgbG9vayB0aHJvdWdoIHRoZSB0b3AtbGV2ZWwgaW1wb3J0cy5cbiAgZm9yIChjb25zdCBub2RlIG9mIHNvdXJjZUZpbGUuc3RhdGVtZW50cykge1xuICAgIGlmICghdHMuaXNJbXBvcnREZWNsYXJhdGlvbihub2RlKSB8fCAhdHMuaXNTdHJpbmdMaXRlcmFsKG5vZGUubW9kdWxlU3BlY2lmaWVyKSB8fFxuICAgICAgICBub2RlLm1vZHVsZVNwZWNpZmllci50ZXh0ICE9PSAnQGFuZ3VsYXIvY29yZScpIHtcbiAgICAgIGNvbnRpbnVlO1xuICAgIH1cblxuICAgIGNvbnN0IG5hbWVkQmluZGluZ3MgPSBub2RlLmltcG9ydENsYXVzZSAmJiBub2RlLmltcG9ydENsYXVzZS5uYW1lZEJpbmRpbmdzO1xuXG4gICAgaWYgKCFuYW1lZEJpbmRpbmdzIHx8ICF0cy5pc05hbWVkSW1wb3J0cyhuYW1lZEJpbmRpbmdzKSkge1xuICAgICAgY29udGludWU7XG4gICAgfVxuXG4gICAgaWYgKGZpbmRJbXBvcnRTcGVjaWZpZXIobmFtZWRCaW5kaW5ncy5lbGVtZW50cywgc3ltYm9sTmFtZSkpIHtcbiAgICAgIHJldHVybiBuYW1lZEJpbmRpbmdzO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBudWxsO1xufVxuXG4vKiogRmluZHMgYW4gaW1wb3J0IHNwZWNpZmllciB3aXRoIGEgcGFydGljdWxhciBuYW1lLCBhY2NvdW50aW5nIGZvciBhbGlhc2VzLiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGZpbmRJbXBvcnRTcGVjaWZpZXIoXG4gICAgZWxlbWVudHM6IHRzLk5vZGVBcnJheTx0cy5JbXBvcnRTcGVjaWZpZXI+LCBpbXBvcnROYW1lOiBzdHJpbmcpIHtcbiAgcmV0dXJuIGVsZW1lbnRzLmZpbmQoZWxlbWVudCA9PiB7XG4gICAgY29uc3Qge25hbWUsIHByb3BlcnR5TmFtZX0gPSBlbGVtZW50O1xuICAgIHJldHVybiBwcm9wZXJ0eU5hbWUgPyBwcm9wZXJ0eU5hbWUudGV4dCA9PT0gaW1wb3J0TmFtZSA6IG5hbWUudGV4dCA9PT0gaW1wb3J0TmFtZTtcbiAgfSkgfHxcbiAgICAgIG51bGw7XG59XG5cbi8qKiBDaGVja3Mgd2hldGhlciBhIG5vZGUgaXMgcmVmZXJyaW5nIHRvIGFuIGltcG9ydCBzcGNpZmllci4gKi9cbmZ1bmN0aW9uIGlzUmVmZXJlbmNlVG9JbXBvcnQoXG4gICAgdHlwZUNoZWNrZXI6IHRzLlR5cGVDaGVja2VyLCBub2RlOiB0cy5Ob2RlLFxuICAgIGltcG9ydFNwZWNpZmllcjogdHMuSW1wb3J0U3BlY2lmaWVyIHwgbnVsbCk6IGJvb2xlYW4ge1xuICBpZiAoaW1wb3J0U3BlY2lmaWVyKSB7XG4gICAgY29uc3Qgbm9kZVN5bWJvbCA9IHR5cGVDaGVja2VyLmdldFR5cGVBdExvY2F0aW9uKG5vZGUpLmdldFN5bWJvbCgpO1xuICAgIGNvbnN0IGltcG9ydFN5bWJvbCA9IHR5cGVDaGVja2VyLmdldFR5cGVBdExvY2F0aW9uKGltcG9ydFNwZWNpZmllcikuZ2V0U3ltYm9sKCk7XG4gICAgcmV0dXJuICEhKG5vZGVTeW1ib2wgJiYgaW1wb3J0U3ltYm9sKSAmJlxuICAgICAgICBub2RlU3ltYm9sLnZhbHVlRGVjbGFyYXRpb24gPT09IGltcG9ydFN5bWJvbC52YWx1ZURlY2xhcmF0aW9uO1xuICB9XG4gIHJldHVybiBmYWxzZTtcbn1cblxuLyoqIEZpbmRzIHRoZSBpZGVudGlmaWVyIHJlZmVycmluZyB0byB0aGUgYFJlbmRlcmVyYCBpbnNpZGUgYSBgZm9yd2FyZFJlZmAgY2FsbCBleHByZXNzaW9uLiAqL1xuZnVuY3Rpb24gZmluZFJlbmRlcmVySWRlbnRpZmllckluRm9yd2FyZFJlZihcbiAgICB0eXBlQ2hlY2tlcjogdHMuVHlwZUNoZWNrZXIsIG5vZGU6IHRzLkNhbGxFeHByZXNzaW9uLFxuICAgIHJlbmRlcmVySW1wb3J0OiB0cy5JbXBvcnRTcGVjaWZpZXIgfCBudWxsKTogdHMuSWRlbnRpZmllcnxudWxsIHtcbiAgY29uc3QgZmlyc3RBcmcgPSBub2RlLmFyZ3VtZW50c1swXTtcblxuICBpZiAodHMuaXNBcnJvd0Z1bmN0aW9uKGZpcnN0QXJnKSkge1xuICAgIC8vIENoZWNrIGlmIHRoZSBmdW5jdGlvbiBpcyBgZm9yd2FyZFJlZigoKSA9PiBSZW5kZXJlcilgLlxuICAgIGlmICh0cy5pc0lkZW50aWZpZXIoZmlyc3RBcmcuYm9keSkgJiZcbiAgICAgICAgaXNSZWZlcmVuY2VUb0ltcG9ydCh0eXBlQ2hlY2tlciwgZmlyc3RBcmcuYm9keSwgcmVuZGVyZXJJbXBvcnQpKSB7XG4gICAgICByZXR1cm4gZmlyc3RBcmcuYm9keTtcbiAgICB9IGVsc2UgaWYgKHRzLmlzQmxvY2soZmlyc3RBcmcuYm9keSkgJiYgdHMuaXNSZXR1cm5TdGF0ZW1lbnQoZmlyc3RBcmcuYm9keS5zdGF0ZW1lbnRzWzBdKSkge1xuICAgICAgLy8gT3RoZXJ3aXNlIGNoZWNrIGlmIHRoZSBleHByZXNzaW9uIGlzIGBmb3J3YXJkUmVmKCgpID0+IHsgcmV0dXJuIFJlbmRlcmVyIH0pYC5cbiAgICAgIGNvbnN0IHJldHVyblN0YXRlbWVudCA9IGZpcnN0QXJnLmJvZHkuc3RhdGVtZW50c1swXSBhcyB0cy5SZXR1cm5TdGF0ZW1lbnQ7XG5cbiAgICAgIGlmIChyZXR1cm5TdGF0ZW1lbnQuZXhwcmVzc2lvbiAmJiB0cy5pc0lkZW50aWZpZXIocmV0dXJuU3RhdGVtZW50LmV4cHJlc3Npb24pICYmXG4gICAgICAgICAgaXNSZWZlcmVuY2VUb0ltcG9ydCh0eXBlQ2hlY2tlciwgcmV0dXJuU3RhdGVtZW50LmV4cHJlc3Npb24sIHJlbmRlcmVySW1wb3J0KSkge1xuICAgICAgICByZXR1cm4gcmV0dXJuU3RhdGVtZW50LmV4cHJlc3Npb247XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIG51bGw7XG59XG4iXX0=