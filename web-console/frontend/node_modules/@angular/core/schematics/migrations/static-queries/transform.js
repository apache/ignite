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
        define("@angular/core/schematics/migrations/static-queries/transform", ["require", "exports", "typescript", "@angular/core/schematics/utils/typescript/property_name", "@angular/core/schematics/migrations/static-queries/angular/query-definition"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const ts = require("typescript");
    const property_name_1 = require("@angular/core/schematics/utils/typescript/property_name");
    const query_definition_1 = require("@angular/core/schematics/migrations/static-queries/angular/query-definition");
    const TODO_SPECIFY_COMMENT = 'TODO: add static flag';
    const TODO_CHECK_COMMENT = 'TODO: check static flag';
    /**
     * Transforms the given query decorator by explicitly specifying the timing based on the
     * determined timing. The updated decorator call expression node will be returned.
     */
    function getTransformedQueryCallExpr(query, timing, createTodo) {
        const queryExpr = query.decorator.node.expression;
        const queryArguments = queryExpr.arguments;
        const queryPropertyAssignments = timing === null ?
            [] :
            [ts.createPropertyAssignment('static', timing === query_definition_1.QueryTiming.STATIC ? ts.createTrue() : ts.createFalse())];
        // If the query decorator is already called with two arguments, we need to
        // keep the existing options untouched and just add the new property if possible.
        if (queryArguments.length === 2) {
            const existingOptions = queryArguments[1];
            const existingOptionsText = existingOptions.getFullText();
            const hasTodoComment = existingOptionsText.includes(TODO_SPECIFY_COMMENT) ||
                existingOptionsText.includes(TODO_CHECK_COMMENT);
            let newOptionsNode;
            let failureMessage = null;
            if (ts.isObjectLiteralExpression(existingOptions)) {
                // In case the options already contains a property for the "static" flag,
                // we just skip this query and leave it untouched.
                if (existingOptions.properties.some(p => !!p.name && property_name_1.getPropertyNameText(p.name) === 'static')) {
                    return null;
                }
                newOptionsNode = ts.updateObjectLiteral(existingOptions, existingOptions.properties.concat(queryPropertyAssignments));
                // In case we want to add a todo and the options do not have the todo
                // yet, we add the query timing todo as synthetic multi-line comment.
                if (createTodo && !hasTodoComment) {
                    addQueryTimingTodoToNode(newOptionsNode, timing === null);
                }
            }
            else {
                // In case the options query parameter is not an object literal expression, and
                // we want to set the query timing, we just preserve the existing query parameter.
                newOptionsNode = existingOptions;
                // We always want to add a TODO in case the query options cannot be updated.
                if (!hasTodoComment) {
                    addQueryTimingTodoToNode(existingOptions, true);
                }
                // If there is a new explicit timing that has been determined for the given query,
                // we create a transformation failure message that shows developers that they need
                // to set the query timing manually to the determined query timing.
                if (timing !== null) {
                    failureMessage = 'Cannot update query to set explicit timing. Please manually ' +
                        `set the query timing to: "{static: ${(timing === query_definition_1.QueryTiming.STATIC).toString()}}"`;
                }
            }
            return {
                failureMessage,
                node: ts.updateCall(queryExpr, queryExpr.expression, queryExpr.typeArguments, [queryArguments[0], newOptionsNode])
            };
        }
        const optionsNode = ts.createObjectLiteral(queryPropertyAssignments);
        if (createTodo) {
            addQueryTimingTodoToNode(optionsNode, timing === null);
        }
        return {
            failureMessage: null,
            node: ts.updateCall(queryExpr, queryExpr.expression, queryExpr.typeArguments, [queryArguments[0], optionsNode])
        };
    }
    exports.getTransformedQueryCallExpr = getTransformedQueryCallExpr;
    /**
     * Adds a to-do to the given TypeScript node which reminds developers to specify
     * an explicit query timing or to double-check the updated timing.
     */
    function addQueryTimingTodoToNode(node, addSpecifyTimingTodo) {
        ts.setSyntheticLeadingComments(node, [{
                pos: -1,
                end: -1,
                hasTrailingNewLine: false,
                kind: ts.SyntaxKind.MultiLineCommentTrivia,
                text: ` ${addSpecifyTimingTodo ? TODO_SPECIFY_COMMENT : TODO_CHECK_COMMENT} `
            }]);
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidHJhbnNmb3JtLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zY2hlbWF0aWNzL21pZ3JhdGlvbnMvc3RhdGljLXF1ZXJpZXMvdHJhbnNmb3JtLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgsaUNBQWlDO0lBQ2pDLDJGQUF5RTtJQUN6RSxrSEFBMEU7SUFTMUUsTUFBTSxvQkFBb0IsR0FBRyx1QkFBdUIsQ0FBQztJQUNyRCxNQUFNLGtCQUFrQixHQUFHLHlCQUF5QixDQUFDO0lBRXJEOzs7T0FHRztJQUNILFNBQWdCLDJCQUEyQixDQUN2QyxLQUF3QixFQUFFLE1BQTBCLEVBQ3BELFVBQW1CO1FBQ3JCLE1BQU0sU0FBUyxHQUFHLEtBQUssQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQztRQUNsRCxNQUFNLGNBQWMsR0FBRyxTQUFTLENBQUMsU0FBUyxDQUFDO1FBQzNDLE1BQU0sd0JBQXdCLEdBQUcsTUFBTSxLQUFLLElBQUksQ0FBQyxDQUFDO1lBQzlDLEVBQUUsQ0FBQyxDQUFDO1lBQ0osQ0FBQyxFQUFFLENBQUMsd0JBQXdCLENBQ3hCLFFBQVEsRUFBRSxNQUFNLEtBQUssOEJBQVcsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxVQUFVLEVBQUUsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLFdBQVcsRUFBRSxDQUFDLENBQUMsQ0FBQztRQUV2RiwwRUFBMEU7UUFDMUUsaUZBQWlGO1FBQ2pGLElBQUksY0FBYyxDQUFDLE1BQU0sS0FBSyxDQUFDLEVBQUU7WUFDL0IsTUFBTSxlQUFlLEdBQUcsY0FBYyxDQUFDLENBQUMsQ0FBQyxDQUFDO1lBQzFDLE1BQU0sbUJBQW1CLEdBQUcsZUFBZSxDQUFDLFdBQVcsRUFBRSxDQUFDO1lBQzFELE1BQU0sY0FBYyxHQUFHLG1CQUFtQixDQUFDLFFBQVEs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KiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCAqIGFzIHRzIGZyb20gJ3R5cGVzY3JpcHQnO1xuaW1wb3J0IHtnZXRQcm9wZXJ0eU5hbWVUZXh0fSBmcm9tICcuLi8uLi91dGlscy90eXBlc2NyaXB0L3Byb3BlcnR5X25hbWUnO1xuaW1wb3J0IHtOZ1F1ZXJ5RGVmaW5pdGlvbiwgUXVlcnlUaW1pbmd9IGZyb20gJy4vYW5ndWxhci9xdWVyeS1kZWZpbml0aW9uJztcblxuZXhwb3J0IHR5cGUgVHJhbnNmb3JtZWRRdWVyeVJlc3VsdCA9IG51bGwgfCB7XG4gIC8qKiBUcmFuc2Zvcm1lZCBjYWxsIGV4cHJlc3Npb24uICovXG4gIG5vZGU6IHRzLkNhbGxFeHByZXNzaW9uO1xuICAvKiogRmFpbHVyZSBtZXNzYWdlIHdoaWNoIGlzIHNldCB3aGVuIHRoZSBxdWVyeSBjb3VsZCBub3QgYmUgdHJhbnNmb3JtZWQgc3VjY2Vzc2Z1bGx5LiAqL1xuICBmYWlsdXJlTWVzc2FnZTogc3RyaW5nfG51bGw7XG59O1xuXG5jb25zdCBUT0RPX1NQRUNJRllfQ09NTUVOVCA9ICdUT0RPOiBhZGQgc3RhdGljIGZsYWcnO1xuY29uc3QgVE9ET19DSEVDS19DT01NRU5UID0gJ1RPRE86IGNoZWNrIHN0YXRpYyBmbGFnJztcblxuLyoqXG4gKiBUcmFuc2Zvcm1zIHRoZSBnaXZlbiBxdWVyeSBkZWNvcmF0b3IgYnkgZXhwbGljaXRseSBzcGVjaWZ5aW5nIHRoZSB0aW1pbmcgYmFzZWQgb24gdGhlXG4gKiBkZXRlcm1pbmVkIHRpbWluZy4gVGhlIHVwZGF0ZWQgZGVjb3JhdG9yIGNhbGwgZXhwcmVzc2lvbiBub2RlIHdpbGwgYmUgcmV0dXJuZWQuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBnZXRUcmFuc2Zvcm1lZFF1ZXJ5Q2FsbEV4cHIoXG4gICAgcXVlcnk6IE5nUXVlcnlEZWZpbml0aW9uLCB0aW1pbmc6IFF1ZXJ5VGltaW5nIHwgbnVsbCxcbiAgICBjcmVhdGVUb2RvOiBib29sZWFuKTogVHJhbnNmb3JtZWRRdWVyeVJlc3VsdCB7XG4gIGNvbnN0IHF1ZXJ5RXhwciA9IHF1ZXJ5LmRlY29yYXRvci5ub2RlLmV4cHJlc3Npb247XG4gIGNvbnN0IHF1ZXJ5QXJndW1lbnRzID0gcXVlcnlFeHByLmFyZ3VtZW50cztcbiAgY29uc3QgcXVlcnlQcm9wZXJ0eUFzc2lnbm1lbnRzID0gdGltaW5nID09PSBudWxsID9cbiAgICAgIFtdIDpcbiAgICAgIFt0cy5jcmVhdGVQcm9wZXJ0eUFzc2lnbm1lbnQoXG4gICAgICAgICAgJ3N0YXRpYycsIHRpbWluZyA9PT0gUXVlcnlUaW1pbmcuU1RBVElDID8gdHMuY3JlYXRlVHJ1ZSgpIDogdHMuY3JlYXRlRmFsc2UoKSldO1xuXG4gIC8vIElmIHRoZSBxdWVyeSBkZWNvcmF0b3IgaXMgYWxyZWFkeSBjYWxsZWQgd2l0aCB0d28gYXJndW1lbnRzLCB3ZSBuZWVkIHRvXG4gIC8vIGtlZXAgdGhlIGV4aXN0aW5nIG9wdGlvbnMgdW50b3VjaGVkIGFuZCBqdXN0IGFkZCB0aGUgbmV3IHByb3BlcnR5IGlmIHBvc3NpYmxlLlxuICBpZiAocXVlcnlBcmd1bWVudHMubGVuZ3RoID09PSAyKSB7XG4gICAgY29uc3QgZXhpc3RpbmdPcHRpb25zID0gcXVlcnlBcmd1bWVudHNbMV07XG4gICAgY29uc3QgZXhpc3RpbmdPcHRpb25zVGV4dCA9IGV4aXN0aW5nT3B0aW9ucy5nZXRGdWxsVGV4dCgpO1xuICAgIGNvbnN0IGhhc1RvZG9Db21tZW50ID0gZXhpc3RpbmdPcHRpb25zVGV4dC5pbmNsdWRlcyhUT0RPX1NQRUNJRllfQ09NTUVOVCkgfHxcbiAgICAgICAgZXhpc3RpbmdPcHRpb25zVGV4dC5pbmNsdWRlcyhUT0RPX0NIRUNLX0NPTU1FTlQpO1xuICAgIGxldCBuZXdPcHRpb25zTm9kZTogdHMuRXhwcmVzc2lvbjtcbiAgICBsZXQgZmFpbHVyZU1lc3NhZ2U6IHN0cmluZ3xudWxsID0gbnVsbDtcblxuICAgIGlmICh0cy5pc09iamVjdExpdGVyYWxFeHByZXNzaW9uKGV4aXN0aW5nT3B0aW9ucykpIHtcbiAgICAgIC8vIEluIGNhc2UgdGhlIG9wdGlvbnMgYWxyZWFkeSBjb250YWlucyBhIHByb3BlcnR5IGZvciB0aGUgXCJzdGF0aWNcIiBmbGFnLFxuICAgICAgLy8gd2UganVzdCBza2lwIHRoaXMgcXVlcnkgYW5kIGxlYXZlIGl0IHVudG91Y2hlZC5cbiAgICAgIGlmIChleGlzdGluZ09wdGlvbnMucHJvcGVydGllcy5zb21lKFxuICAgICAgICAgICAgICBwID0+ICEhcC5uYW1lICYmIGdldFByb3BlcnR5TmFtZVRleHQocC5uYW1lKSA9PT0gJ3N0YXRpYycpKSB7XG4gICAgICAgIHJldHVybiBudWxsO1xuICAgICAgfVxuXG4gICAgICBuZXdPcHRpb25zTm9kZSA9IHRzLnVwZGF0ZU9iamVjdExpdGVyYWwoXG4gICAgICAgICAgZXhpc3RpbmdPcHRpb25zLCBleGlzdGluZ09wdGlvbnMucHJvcGVydGllcy5jb25jYXQocXVlcnlQcm9wZXJ0eUFzc2lnbm1lbnRzKSk7XG5cbiAgICAgIC8vIEluIGNhc2Ugd2Ugd2FudCB0byBhZGQgYSB0b2RvIGFuZCB0aGUgb3B0aW9ucyBkbyBub3QgaGF2ZSB0aGUgdG9kb1xuICAgICAgLy8geWV0LCB3ZSBhZGQgdGhlIHF1ZXJ5IHRpbWluZyB0b2RvIGFzIHN5bnRoZXRpYyBtdWx0aS1saW5lIGNvbW1lbnQuXG4gICAgICBpZiAoY3JlYXRlVG9kbyAmJiAhaGFzVG9kb0NvbW1lbnQpIHtcbiAgICAgICAgYWRkUXVlcnlUaW1pbmdUb2RvVG9Ob2RlKG5ld09wdGlvbnNOb2RlLCB0aW1pbmcgPT09IG51bGwpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICAvLyBJbiBjYXNlIHRoZSBvcHRpb25zIHF1ZXJ5IHBhcmFtZXRlciBpcyBub3QgYW4gb2JqZWN0IGxpdGVyYWwgZXhwcmVzc2lvbiwgYW5kXG4gICAgICAvLyB3ZSB3YW50IHRvIHNldCB0aGUgcXVlcnkgdGltaW5nLCB3ZSBqdXN0IHByZXNlcnZlIHRoZSBleGlzdGluZyBxdWVyeSBwYXJhbWV0ZXIuXG4gICAgICBuZXdPcHRpb25zTm9kZSA9IGV4aXN0aW5nT3B0aW9ucztcbiAgICAgIC8vIFdlIGFsd2F5cyB3YW50IHRvIGFkZCBhIFRPRE8gaW4gY2FzZSB0aGUgcXVlcnkgb3B0aW9ucyBjYW5ub3QgYmUgdXBkYXRlZC5cbiAgICAgIGlmICghaGFzVG9kb0NvbW1lbnQpIHtcbiAgICAgICAgYWRkUXVlcnlUaW1pbmdUb2RvVG9Ob2RlKGV4aXN0aW5nT3B0aW9ucywgdHJ1ZSk7XG4gICAgICB9XG4gICAgICAvLyBJZiB0aGVyZSBpcyBhIG5ldyBleHBsaWNpdCB0aW1pbmcgdGhhdCBoYXMgYmVlbiBkZXRlcm1pbmVkIGZvciB0aGUgZ2l2ZW4gcXVlcnksXG4gICAgICAvLyB3ZSBjcmVhdGUgYSB0cmFuc2Zvcm1hdGlvbiBmYWlsdXJlIG1lc3NhZ2UgdGhhdCBzaG93cyBkZXZlbG9wZXJzIHRoYXQgdGhleSBuZWVkXG4gICAgICAvLyB0byBzZXQgdGhlIHF1ZXJ5IHRpbWluZyBtYW51YWxseSB0byB0aGUgZGV0ZXJtaW5lZCBxdWVyeSB0aW1pbmcuXG4gICAgICBpZiAodGltaW5nICE9PSBudWxsKSB7XG4gICAgICAgIGZhaWx1cmVNZXNzYWdlID0gJ0Nhbm5vdCB1cGRhdGUgcXVlcnkgdG8gc2V0IGV4cGxpY2l0IHRpbWluZy4gUGxlYXNlIG1hbnVhbGx5ICcgK1xuICAgICAgICAgICAgYHNldCB0aGUgcXVlcnkgdGltaW5nIHRvOiBcIntzdGF0aWM6ICR7KHRpbWluZyA9PT0gUXVlcnlUaW1pbmcuU1RBVElDKS50b1N0cmluZygpfX1cImA7XG4gICAgICB9XG4gICAgfVxuXG4gICAgcmV0dXJuIHtcbiAgICAgIGZhaWx1cmVNZXNzYWdlLFxuICAgICAgbm9kZTogdHMudXBkYXRlQ2FsbChcbiAgICAgICAgICBxdWVyeUV4cHIsIHF1ZXJ5RXhwci5leHByZXNzaW9uLCBxdWVyeUV4cHIudHlwZUFyZ3VtZW50cyxcbiAgICAgICAgICBbcXVlcnlBcmd1bWVudHNbMF0sIG5ld09wdGlvbnNOb2RlICFdKVxuICAgIH07XG4gIH1cblxuICBjb25zdCBvcHRpb25zTm9kZSA9IHRzLmNyZWF0ZU9iamVjdExpdGVyYWwocXVlcnlQcm9wZXJ0eUFzc2lnbm1lbnRzKTtcblxuICBpZiAoY3JlYXRlVG9kbykge1xuICAgIGFkZFF1ZXJ5VGltaW5nVG9kb1RvTm9kZShvcHRpb25zTm9kZSwgdGltaW5nID09PSBudWxsKTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgZmFpbHVyZU1lc3NhZ2U6IG51bGwsXG4gICAgbm9kZTogdHMudXBkYXRlQ2FsbChcbiAgICAgICAgcXVlcnlFeHByLCBxdWVyeUV4cHIuZXhwcmVzc2lvbiwgcXVlcnlFeHByLnR5cGVBcmd1bWVudHMsXG4gICAgICAgIFtxdWVyeUFyZ3VtZW50c1swXSwgb3B0aW9uc05vZGVdKVxuICB9O1xufVxuXG4vKipcbiAqIEFkZHMgYSB0by1kbyB0byB0aGUgZ2l2ZW4gVHlwZVNjcmlwdCBub2RlIHdoaWNoIHJlbWluZHMgZGV2ZWxvcGVycyB0byBzcGVjaWZ5XG4gKiBhbiBleHBsaWNpdCBxdWVyeSB0aW1pbmcgb3IgdG8gZG91YmxlLWNoZWNrIHRoZSB1cGRhdGVkIHRpbWluZy5cbiAqL1xuZnVuY3Rpb24gYWRkUXVlcnlUaW1pbmdUb2RvVG9Ob2RlKG5vZGU6IHRzLk5vZGUsIGFkZFNwZWNpZnlUaW1pbmdUb2RvOiBib29sZWFuKSB7XG4gIHRzLnNldFN5bnRoZXRpY0xlYWRpbmdDb21tZW50cyhcbiAgICAgIG5vZGUsIFt7XG4gICAgICAgIHBvczogLTEsXG4gICAgICAgIGVuZDogLTEsXG4gICAgICAgIGhhc1RyYWlsaW5nTmV3TGluZTogZmFsc2UsXG4gICAgICAgIGtpbmQ6IHRzLlN5bnRheEtpbmQuTXVsdGlMaW5lQ29tbWVudFRyaXZpYSxcbiAgICAgICAgdGV4dDogYCAke2FkZFNwZWNpZnlUaW1pbmdUb2RvID8gVE9ET19TUEVDSUZZX0NPTU1FTlQgOiBUT0RPX0NIRUNLX0NPTU1FTlR9IGBcbiAgICAgIH1dKTtcbn1cbiJdfQ==