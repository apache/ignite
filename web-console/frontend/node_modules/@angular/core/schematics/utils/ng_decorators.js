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
        define("@angular/core/schematics/utils/ng_decorators", ["require", "exports", "@angular/core/schematics/utils/typescript/decorators"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const decorators_1 = require("@angular/core/schematics/utils/typescript/decorators");
    /**
     * Gets all decorators which are imported from an Angular package (e.g. "@angular/core")
     * from a list of decorators.
     */
    function getAngularDecorators(typeChecker, decorators) {
        return decorators.map(node => ({ node, importData: decorators_1.getCallDecoratorImport(typeChecker, node) }))
            .filter(({ importData }) => importData && importData.importModule.startsWith('@angular/'))
            .map(({ node, importData }) => ({
            node: node,
            name: importData.name,
            importNode: importData.node
        }));
    }
    exports.getAngularDecorators = getAngularDecorators;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfZGVjb3JhdG9ycy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc2NoZW1hdGljcy91dGlscy9uZ19kZWNvcmF0b3JzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBR0gscUZBQStEO0lBWS9EOzs7T0FHRztJQUNILFNBQWdCLG9CQUFvQixDQUNoQyxXQUEyQixFQUFFLFVBQXVDO1FBQ3RFLE9BQU8sVUFBVSxDQUFDLEdBQUcsQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUMsRUFBQyxJQUFJLEVBQUUsVUFBVSxFQUFFLG1DQUFzQixDQUFDLFdBQVcsRUFBRSxJQUFJLENBQUMsRUFBQyxDQUFDLENBQUM7YUFDekYsTUFBTSxDQUFDLENBQUMsRUFBQyxVQUFVLEVBQUMsRUFBRSxFQUFFLENBQUMsVUFBVSxJQUFJLFVBQVUsQ0FBQyxZQUFZLENBQUMsVUFBVSxDQUFDLFdBQVcsQ0FBQyxDQUFDO2FBQ3ZGLEdBQUcsQ0FBQyxDQUFDLEVBQUMsSUFBSSxFQUFFLFVBQVUsRUFBQyxFQUFFLEVBQUUsQ0FBQyxDQUFDO1lBQ3ZCLElBQUksRUFBRSxJQUErQjtZQUNyQyxJQUFJLEVBQUUsVUFBWSxDQUFDLElBQUk7WUFDdkIsVUFBVSxFQUFFLFVBQVksQ0FBQyxJQUFJO1NBQzlCLENBQUMsQ0FBQyxDQUFDO0lBQ2YsQ0FBQztJQVRELG9EQVNDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQgKiBhcyB0cyBmcm9tICd0eXBlc2NyaXB0JztcbmltcG9ydCB7Z2V0Q2FsbERlY29yYXRvckltcG9ydH0gZnJvbSAnLi90eXBlc2NyaXB0L2RlY29yYXRvcnMnO1xuXG5leHBvcnQgdHlwZSBDYWxsRXhwcmVzc2lvbkRlY29yYXRvciA9IHRzLkRlY29yYXRvciAmIHtcbiAgZXhwcmVzc2lvbjogdHMuQ2FsbEV4cHJlc3Npb247XG59O1xuXG5leHBvcnQgaW50ZXJmYWNlIE5nRGVjb3JhdG9yIHtcbiAgbmFtZTogc3RyaW5nO1xuICBub2RlOiBDYWxsRXhwcmVzc2lvbkRlY29yYXRvcjtcbiAgaW1wb3J0Tm9kZTogdHMuSW1wb3J0RGVjbGFyYXRpb247XG59XG5cbi8qKlxuICogR2V0cyBhbGwgZGVjb3JhdG9ycyB3aGljaCBhcmUgaW1wb3J0ZWQgZnJvbSBhbiBBbmd1bGFyIHBhY2thZ2UgKGUuZy4gXCJAYW5ndWxhci9jb3JlXCIpXG4gKiBmcm9tIGEgbGlzdCBvZiBkZWNvcmF0b3JzLlxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0QW5ndWxhckRlY29yYXRvcnMoXG4gICAgdHlwZUNoZWNrZXI6IHRzLlR5cGVDaGVja2VyLCBkZWNvcmF0b3JzOiBSZWFkb25seUFycmF5PHRzLkRlY29yYXRvcj4pOiBOZ0RlY29yYXRvcltdIHtcbiAgcmV0dXJuIGRlY29yYXRvcnMubWFwKG5vZGUgPT4gKHtub2RlLCBpbXBvcnREYXRhOiBnZXRDYWxsRGVjb3JhdG9ySW1wb3J0KHR5cGVDaGVja2VyLCBub2RlKX0pKVxuICAgICAgLmZpbHRlcigoe2ltcG9ydERhdGF9KSA9PiBpbXBvcnREYXRhICYmIGltcG9ydERhdGEuaW1wb3J0TW9kdWxlLnN0YXJ0c1dpdGgoJ0Bhbmd1bGFyLycpKVxuICAgICAgLm1hcCgoe25vZGUsIGltcG9ydERhdGF9KSA9PiAoe1xuICAgICAgICAgICAgIG5vZGU6IG5vZGUgYXMgQ2FsbEV4cHJlc3Npb25EZWNvcmF0b3IsXG4gICAgICAgICAgICAgbmFtZTogaW1wb3J0RGF0YSAhLm5hbWUsXG4gICAgICAgICAgICAgaW1wb3J0Tm9kZTogaW1wb3J0RGF0YSAhLm5vZGVcbiAgICAgICAgICAgfSkpO1xufVxuIl19