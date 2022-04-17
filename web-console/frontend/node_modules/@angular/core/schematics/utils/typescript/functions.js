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
        define("@angular/core/schematics/utils/typescript/functions", ["require", "exports", "typescript"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const ts = require("typescript");
    /** Checks whether a given node is a function like declaration. */
    function isFunctionLikeDeclaration(node) {
        return ts.isFunctionDeclaration(node) || ts.isMethodDeclaration(node) ||
            ts.isArrowFunction(node) || ts.isFunctionExpression(node) ||
            ts.isGetAccessorDeclaration(node) || ts.isSetAccessorDeclaration(node);
    }
    exports.isFunctionLikeDeclaration = isFunctionLikeDeclaration;
    /**
     * Unwraps a given expression TypeScript node. Expressions can be wrapped within multiple
     * parentheses. e.g. "(((({exp}))))()". The function should return the TypeScript node
     * referring to the inner expression. e.g "exp".
     */
    function unwrapExpression(node) {
        if (ts.isParenthesizedExpression(node)) {
            return unwrapExpression(node.expression);
        }
        else {
            return node;
        }
    }
    exports.unwrapExpression = unwrapExpression;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZnVuY3Rpb25zLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zY2hlbWF0aWNzL3V0aWxzL3R5cGVzY3JpcHQvZnVuY3Rpb25zLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgsaUNBQWlDO0lBRWpDLGtFQUFrRTtJQUNsRSxTQUFnQix5QkFBeUIsQ0FBQyxJQUFhO1FBQ3JELE9BQU8sRUFBRSxDQUFDLHFCQUFxQixDQUFDLElBQUksQ0FBQyxJQUFJLEVBQUUsQ0FBQyxtQkFBbUIsQ0FBQyxJQUFJLENBQUM7WUFDakUsRUFBRSxDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsSUFBSSxFQUFFLENBQUMsb0JBQW9CLENBQUMsSUFBSSxDQUFDO1lBQ3pELEVBQUUsQ0FBQyx3QkFBd0IsQ0FBQyxJQUFJLENBQUMsSUFBSSxFQUFFLENBQUMsd0JBQXdCLENBQUMsSUFBSSxDQUFDLENBQUM7SUFDN0UsQ0FBQztJQUpELDhEQUlDO0lBRUQ7Ozs7T0FJRztJQUNILFNBQWdCLGdCQUFnQixDQUFDLElBQWdEO1FBQy9FLElBQUksRUFBRSxDQUFDLHlCQUF5QixDQUFDLElBQUksQ0FBQyxFQUFFO1lBQ3RDLE9BQU8sZ0JBQWdCLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDO1NBQzFDO2FBQU07WUFDTCxPQUFPLElBQUksQ0FBQztTQUNiO0lBQ0gsQ0FBQztJQU5ELDRDQU1DIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQgKiBhcyB0cyBmcm9tICd0eXBlc2NyaXB0JztcblxuLyoqIENoZWNrcyB3aGV0aGVyIGEgZ2l2ZW4gbm9kZSBpcyBhIGZ1bmN0aW9uIGxpa2UgZGVjbGFyYXRpb24uICovXG5leHBvcnQgZnVuY3Rpb24gaXNGdW5jdGlvbkxpa2VEZWNsYXJhdGlvbihub2RlOiB0cy5Ob2RlKTogbm9kZSBpcyB0cy5GdW5jdGlvbkxpa2VEZWNsYXJhdGlvbiB7XG4gIHJldHVybiB0cy5pc0Z1bmN0aW9uRGVjbGFyYXRpb24obm9kZSkgfHwgdHMuaXNNZXRob2REZWNsYXJhdGlvbihub2RlKSB8fFxuICAgICAgdHMuaXNBcnJvd0Z1bmN0aW9uKG5vZGUpIHx8IHRzLmlzRnVuY3Rpb25FeHByZXNzaW9uKG5vZGUpIHx8XG4gICAgICB0cy5pc0dldEFjY2Vzc29yRGVjbGFyYXRpb24obm9kZSkgfHwgdHMuaXNTZXRBY2Nlc3NvckRlY2xhcmF0aW9uKG5vZGUpO1xufVxuXG4vKipcbiAqIFVud3JhcHMgYSBnaXZlbiBleHByZXNzaW9uIFR5cGVTY3JpcHQgbm9kZS4gRXhwcmVzc2lvbnMgY2FuIGJlIHdyYXBwZWQgd2l0aGluIG11bHRpcGxlXG4gKiBwYXJlbnRoZXNlcy4gZS5nLiBcIigoKCh7ZXhwfSkpKSkoKVwiLiBUaGUgZnVuY3Rpb24gc2hvdWxkIHJldHVybiB0aGUgVHlwZVNjcmlwdCBub2RlXG4gKiByZWZlcnJpbmcgdG8gdGhlIGlubmVyIGV4cHJlc3Npb24uIGUuZyBcImV4cFwiLlxuICovXG5leHBvcnQgZnVuY3Rpb24gdW53cmFwRXhwcmVzc2lvbihub2RlOiB0cy5FeHByZXNzaW9uIHwgdHMuUGFyZW50aGVzaXplZEV4cHJlc3Npb24pOiB0cy5FeHByZXNzaW9uIHtcbiAgaWYgKHRzLmlzUGFyZW50aGVzaXplZEV4cHJlc3Npb24obm9kZSkpIHtcbiAgICByZXR1cm4gdW53cmFwRXhwcmVzc2lvbihub2RlLmV4cHJlc3Npb24pO1xuICB9IGVsc2Uge1xuICAgIHJldHVybiBub2RlO1xuICB9XG59XG4iXX0=