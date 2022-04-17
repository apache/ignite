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
        define("@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/super_class_context", ["require", "exports", "typescript", "@angular/core/schematics/utils/typescript/functions", "@angular/core/schematics/utils/typescript/nodes", "@angular/core/schematics/utils/typescript/property_name", "@angular/core/schematics/migrations/static-queries/angular/super_class"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const ts = require("typescript");
    const functions_1 = require("@angular/core/schematics/utils/typescript/functions");
    const nodes_1 = require("@angular/core/schematics/utils/typescript/nodes");
    const property_name_1 = require("@angular/core/schematics/utils/typescript/property_name");
    const super_class_1 = require("@angular/core/schematics/migrations/static-queries/angular/super_class");
    /**
     * Updates the specified function context to map abstract super-class class members
     * to their implementation TypeScript nodes. This allows us to run the declaration visitor
     * for the super class with the context of the "baseClass" (e.g. with implemented abstract
     * class members)
     */
    function updateSuperClassAbstractMembersContext(baseClass, context, classMetadataMap) {
        super_class_1.getSuperClassDeclarations(baseClass, classMetadataMap).forEach(superClassDecl => {
            superClassDecl.members.forEach(superClassMember => {
                if (!superClassMember.name || !nodes_1.hasModifier(superClassMember, ts.SyntaxKind.AbstractKeyword)) {
                    return;
                }
                // Find the matching implementation of the abstract declaration from the super class.
                const baseClassImpl = baseClass.members.find(baseClassMethod => !!baseClassMethod.name &&
                    property_name_1.getPropertyNameText(baseClassMethod.name) ===
                        property_name_1.getPropertyNameText(superClassMember.name));
                if (!baseClassImpl || !functions_1.isFunctionLikeDeclaration(baseClassImpl) || !baseClassImpl.body) {
                    return;
                }
                if (!context.has(superClassMember)) {
                    context.set(superClassMember, baseClassImpl);
                }
            });
        });
    }
    exports.updateSuperClassAbstractMembersContext = updateSuperClassAbstractMembersContext;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3VwZXJfY2xhc3NfY29udGV4dC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc2NoZW1hdGljcy9taWdyYXRpb25zL3N0YXRpYy1xdWVyaWVzL3N0cmF0ZWdpZXMvdXNhZ2Vfc3RyYXRlZ3kvc3VwZXJfY2xhc3NfY29udGV4dC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILGlDQUFpQztJQUVqQyxtRkFBaUY7SUFDakYsMkVBQStEO0lBQy9ELDJGQUErRTtJQUUvRSx3R0FBb0U7SUFLcEU7Ozs7O09BS0c7SUFDSCxTQUFnQixzQ0FBc0MsQ0FDbEQsU0FBOEIsRUFBRSxPQUF3QixFQUFFLGdCQUFrQztRQUM5Rix1Q0FBeUIsQ0FBQyxTQUFTLEVBQUUsZ0JBQWdCLENBQUMsQ0FBQyxPQUFPLENBQUMsY0FBYyxDQUFDLEVBQUU7WUFDOUUsY0FBYyxDQUFDLE9BQU8sQ0FBQyxPQUFPLENBQUMsZ0JBQWdCLENBQUMsRUFBRTtnQkFDaEQsSUFBSSxDQUFDLGdCQUFnQixDQUFDLElBQUksSUFBSSxDQUFDLG1CQUFXLENBQUMsZ0JBQWdCLEVBQUUsRUFBRSxDQUFDLFVBQVUsQ0FBQyxlQUFlLENBQUMsRUFBRTtvQkFDM0YsT0FBTztpQkFDUjtnQkFFRCxxRkFBcUY7Z0JBQ3JGLE1BQU0sYUFBYSxHQUFHLFNBQVMsQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUN4QyxlQUFlLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxlQUFlLENBQUMsSUFBSTtvQkFDckMsbUNBQW1CLENBQUMsZUFBZSxDQUFDLElBQUksQ0FBQzt3QkFDckMsbUNBQW1CLENBQUMsZ0JBQWdCLENBQUMsSUFBTSxDQUFDLENBQUMsQ0FBQztnQkFFMUQsSUFBSSxDQUFDLGFBQWEsSUFBSSxDQUFDLHFDQUF5QixDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsYUFBYSxDQUFDLElBQUksRUFBRTtvQkFDdEYsT0FBTztpQkFDUjtnQkFFRCxJQUFJLENBQUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxnQkFBZ0IsQ0FBQyxFQUFFO29CQUNsQyxPQUFPLENBQUMsR0FBRyxDQUFDLGdCQUFnQixFQUFFLGFBQWEsQ0FBQyxDQUFDO2lCQUM5QztZQUNILENBQUMsQ0FBQyxDQUFDO1FBQ0wsQ0FBQyxDQUFDLENBQUM7SUFDTCxDQUFDO0lBdkJELHdGQXVCQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0ICogYXMgdHMgZnJvbSAndHlwZXNjcmlwdCc7XG5cbmltcG9ydCB7aXNGdW5jdGlvbkxpa2VEZWNsYXJhdGlvbn0gZnJvbSAnLi4vLi4vLi4vLi4vdXRpbHMvdHlwZXNjcmlwdC9mdW5jdGlvbnMnO1xuaW1wb3J0IHtoYXNNb2RpZmllcn0gZnJvbSAnLi4vLi4vLi4vLi4vdXRpbHMvdHlwZXNjcmlwdC9ub2Rlcyc7XG5pbXBvcnQge2dldFByb3BlcnR5TmFtZVRleHR9IGZyb20gJy4uLy4uLy4uLy4uL3V0aWxzL3R5cGVzY3JpcHQvcHJvcGVydHlfbmFtZSc7XG5pbXBvcnQge0NsYXNzTWV0YWRhdGFNYXB9IGZyb20gJy4uLy4uL2FuZ3VsYXIvbmdfcXVlcnlfdmlzaXRvcic7XG5pbXBvcnQge2dldFN1cGVyQ2xhc3NEZWNsYXJhdGlvbnN9IGZyb20gJy4uLy4uL2FuZ3VsYXIvc3VwZXJfY2xhc3MnO1xuXG5pbXBvcnQge0Z1bmN0aW9uQ29udGV4dH0gZnJvbSAnLi9kZWNsYXJhdGlvbl91c2FnZV92aXNpdG9yJztcblxuXG4vKipcbiAqIFVwZGF0ZXMgdGhlIHNwZWNpZmllZCBmdW5jdGlvbiBjb250ZXh0IHRvIG1hcCBhYnN0cmFjdCBzdXBlci1jbGFzcyBjbGFzcyBtZW1iZXJzXG4gKiB0byB0aGVpciBpbXBsZW1lbnRhdGlvbiBUeXBlU2NyaXB0IG5vZGVzLiBUaGlzIGFsbG93cyB1cyB0byBydW4gdGhlIGRlY2xhcmF0aW9uIHZpc2l0b3JcbiAqIGZvciB0aGUgc3VwZXIgY2xhc3Mgd2l0aCB0aGUgY29udGV4dCBvZiB0aGUgXCJiYXNlQ2xhc3NcIiAoZS5nLiB3aXRoIGltcGxlbWVudGVkIGFic3RyYWN0XG4gKiBjbGFzcyBtZW1iZXJzKVxuICovXG5leHBvcnQgZnVuY3Rpb24gdXBkYXRlU3VwZXJDbGFzc0Fic3RyYWN0TWVtYmVyc0NvbnRleHQoXG4gICAgYmFzZUNsYXNzOiB0cy5DbGFzc0RlY2xhcmF0aW9uLCBjb250ZXh0OiBGdW5jdGlvbkNvbnRleHQsIGNsYXNzTWV0YWRhdGFNYXA6IENsYXNzTWV0YWRhdGFNYXApIHtcbiAgZ2V0U3VwZXJDbGFzc0RlY2xhcmF0aW9ucyhiYXNlQ2xhc3MsIGNsYXNzTWV0YWRhdGFNYXApLmZvckVhY2goc3VwZXJDbGFzc0RlY2wgPT4ge1xuICAgIHN1cGVyQ2xhc3NEZWNsLm1lbWJlcnMuZm9yRWFjaChzdXBlckNsYXNzTWVtYmVyID0+IHtcbiAgICAgIGlmICghc3VwZXJDbGFzc01lbWJlci5uYW1lIHx8ICFoYXNNb2RpZmllcihzdXBlckNsYXNzTWVtYmVyLCB0cy5TeW50YXhLaW5kLkFic3RyYWN0S2V5d29yZCkpIHtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuXG4gICAgICAvLyBGaW5kIHRoZSBtYXRjaGluZyBpbXBsZW1lbnRhdGlvbiBvZiB0aGUgYWJzdHJhY3QgZGVjbGFyYXRpb24gZnJvbSB0aGUgc3VwZXIgY2xhc3MuXG4gICAgICBjb25zdCBiYXNlQ2xhc3NJbXBsID0gYmFzZUNsYXNzLm1lbWJlcnMuZmluZChcbiAgICAgICAgICBiYXNlQ2xhc3NNZXRob2QgPT4gISFiYXNlQ2xhc3NNZXRob2QubmFtZSAmJlxuICAgICAgICAgICAgICBnZXRQcm9wZXJ0eU5hbWVUZXh0KGJhc2VDbGFzc01ldGhvZC5uYW1lKSA9PT1cbiAgICAgICAgICAgICAgICAgIGdldFByb3BlcnR5TmFtZVRleHQoc3VwZXJDbGFzc01lbWJlci5uYW1lICEpKTtcblxuICAgICAgaWYgKCFiYXNlQ2xhc3NJbXBsIHx8ICFpc0Z1bmN0aW9uTGlrZURlY2xhcmF0aW9uKGJhc2VDbGFzc0ltcGwpIHx8ICFiYXNlQ2xhc3NJbXBsLmJvZHkpIHtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuXG4gICAgICBpZiAoIWNvbnRleHQuaGFzKHN1cGVyQ2xhc3NNZW1iZXIpKSB7XG4gICAgICAgIGNvbnRleHQuc2V0KHN1cGVyQ2xhc3NNZW1iZXIsIGJhc2VDbGFzc0ltcGwpO1xuICAgICAgfVxuICAgIH0pO1xuICB9KTtcbn1cbiJdfQ==