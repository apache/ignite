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
        define("@angular/core/schematics/utils/typescript/class_declaration", ["require", "exports", "typescript"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const ts = require("typescript");
    /** Determines the base type identifiers of a specified class declaration. */
    function getBaseTypeIdentifiers(node) {
        if (!node.heritageClauses) {
            return null;
        }
        return node.heritageClauses.filter(clause => clause.token === ts.SyntaxKind.ExtendsKeyword)
            .reduce((types, clause) => types.concat(clause.types), [])
            .map(typeExpression => typeExpression.expression)
            .filter(ts.isIdentifier);
    }
    exports.getBaseTypeIdentifiers = getBaseTypeIdentifiers;
    /** Gets the first found parent class declaration of a given node. */
    function findParentClassDeclaration(node) {
        while (!ts.isClassDeclaration(node)) {
            if (ts.isSourceFile(node)) {
                return null;
            }
            node = node.parent;
        }
        return node;
    }
    exports.findParentClassDeclaration = findParentClassDeclaration;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY2xhc3NfZGVjbGFyYXRpb24uanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NjaGVtYXRpY3MvdXRpbHMvdHlwZXNjcmlwdC9jbGFzc19kZWNsYXJhdGlvbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILGlDQUFpQztJQUVqQyw2RUFBNkU7SUFDN0UsU0FBZ0Isc0JBQXNCLENBQUMsSUFBeUI7UUFDOUQsSUFBSSxDQUFDLElBQUksQ0FBQyxlQUFlLEVBQUU7WUFDekIsT0FBTyxJQUFJLENBQUM7U0FDYjtRQUVELE9BQU8sSUFBSSxDQUFDLGVBQWUsQ0FBQyxNQUFNLENBQUMsTUFBTSxDQUFDLEVBQUUsQ0FBQyxNQUFNLENBQUMsS0FBSyxLQUFLLEVBQUUsQ0FBQyxVQUFVLENBQUMsY0FBYyxDQUFDO2FBQ3RGLE1BQU0sQ0FBQyxDQUFDLEtBQUssRUFBRSxNQUFNLEVBQUUsRUFBRSxDQUFDLEtBQUssQ0FBQyxNQUFNLENBQUMsTUFBTSxDQUFDLEtBQUssQ0FBQyxFQUFFLEVBQXNDLENBQUM7YUFDN0YsR0FBRyxDQUFDLGNBQWMsQ0FBQyxFQUFFLENBQUMsY0FBYyxDQUFDLFVBQVUsQ0FBQzthQUNoRCxNQUFNLENBQUMsRUFBRSxDQUFDLFlBQVksQ0FBQyxDQUFDO0lBQy9CLENBQUM7SUFURCx3REFTQztJQUVELHFFQUFxRTtJQUNyRSxTQUFnQiwwQkFBMEIsQ0FBQyxJQUFhO1FBQ3RELE9BQU8sQ0FBQyxFQUFFLENBQUMsa0JBQWtCLENBQUMsSUFBSSxDQUFDLEVBQUU7WUFDbkMsSUFBSSxFQUFFLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxFQUFFO2dCQUN6QixPQUFPLElBQUksQ0FBQzthQUNiO1lBQ0QsSUFBSSxHQUFHLElBQUksQ0FBQyxNQUFNLENBQUM7U0FDcEI7UUFDRCxPQUFPLElBQUksQ0FBQztJQUNkLENBQUM7SUFSRCxnRUFRQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0ICogYXMgdHMgZnJvbSAndHlwZXNjcmlwdCc7XG5cbi8qKiBEZXRlcm1pbmVzIHRoZSBiYXNlIHR5cGUgaWRlbnRpZmllcnMgb2YgYSBzcGVjaWZpZWQgY2xhc3MgZGVjbGFyYXRpb24uICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0QmFzZVR5cGVJZGVudGlmaWVycyhub2RlOiB0cy5DbGFzc0RlY2xhcmF0aW9uKTogdHMuSWRlbnRpZmllcltdfG51bGwge1xuICBpZiAoIW5vZGUuaGVyaXRhZ2VDbGF1c2VzKSB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICByZXR1cm4gbm9kZS5oZXJpdGFnZUNsYXVzZXMuZmlsdGVyKGNsYXVzZSA9PiBjbGF1c2UudG9rZW4gPT09IHRzLlN5bnRheEtpbmQuRXh0ZW5kc0tleXdvcmQpXG4gICAgICAucmVkdWNlKCh0eXBlcywgY2xhdXNlKSA9PiB0eXBlcy5jb25jYXQoY2xhdXNlLnR5cGVzKSwgW10gYXMgdHMuRXhwcmVzc2lvbldpdGhUeXBlQXJndW1lbnRzW10pXG4gICAgICAubWFwKHR5cGVFeHByZXNzaW9uID0+IHR5cGVFeHByZXNzaW9uLmV4cHJlc3Npb24pXG4gICAgICAuZmlsdGVyKHRzLmlzSWRlbnRpZmllcik7XG59XG5cbi8qKiBHZXRzIHRoZSBmaXJzdCBmb3VuZCBwYXJlbnQgY2xhc3MgZGVjbGFyYXRpb24gb2YgYSBnaXZlbiBub2RlLiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGZpbmRQYXJlbnRDbGFzc0RlY2xhcmF0aW9uKG5vZGU6IHRzLk5vZGUpOiB0cy5DbGFzc0RlY2xhcmF0aW9ufG51bGwge1xuICB3aGlsZSAoIXRzLmlzQ2xhc3NEZWNsYXJhdGlvbihub2RlKSkge1xuICAgIGlmICh0cy5pc1NvdXJjZUZpbGUobm9kZSkpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgICBub2RlID0gbm9kZS5wYXJlbnQ7XG4gIH1cbiAgcmV0dXJuIG5vZGU7XG59XG4iXX0=