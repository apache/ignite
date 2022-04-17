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
        define("@angular/core/schematics/migrations/renderer-to-renderer2", ["require", "exports", "@angular-devkit/schematics", "path", "typescript", "@angular/core/schematics/utils/project_tsconfig_paths", "@angular/core/schematics/utils/typescript/parse_tsconfig", "@angular/core/schematics/migrations/renderer-to-renderer2/helpers", "@angular/core/schematics/migrations/renderer-to-renderer2/migration", "@angular/core/schematics/migrations/renderer-to-renderer2/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const schematics_1 = require("@angular-devkit/schematics");
    const path_1 = require("path");
    const ts = require("typescript");
    const project_tsconfig_paths_1 = require("@angular/core/schematics/utils/project_tsconfig_paths");
    const parse_tsconfig_1 = require("@angular/core/schematics/utils/typescript/parse_tsconfig");
    const helpers_1 = require("@angular/core/schematics/migrations/renderer-to-renderer2/helpers");
    const migration_1 = require("@angular/core/schematics/migrations/renderer-to-renderer2/migration");
    const util_1 = require("@angular/core/schematics/migrations/renderer-to-renderer2/util");
    /**
     * Migration that switches from `Renderer` to `Renderer2`. More information on how it works:
     * https://hackmd.angular.io/UTzUZTnPRA-cSa_4mHyfYw
     */
    function default_1() {
        return (tree, context) => {
            const { buildPaths, testPaths } = project_tsconfig_paths_1.getProjectTsConfigPaths(tree);
            const basePath = process.cwd();
            const allPaths = [...buildPaths, ...testPaths];
            const logger = context.logger;
            logger.info('------ Renderer to Renderer2 Migration ------');
            logger.info('As of Angular 9, the Renderer class is no longer available.');
            logger.info('Renderer2 should be used instead.');
            if (!allPaths.length) {
                throw new schematics_1.SchematicsException('Could not find any tsconfig file. Cannot migrate Renderer usages to Renderer2.');
            }
            for (const tsconfigPath of allPaths) {
                runRendererToRenderer2Migration(tree, tsconfigPath, basePath);
            }
        };
    }
    exports.default = default_1;
    function runRendererToRenderer2Migration(tree, tsconfigPath, basePath) {
        const parsed = parse_tsconfig_1.parseTsconfigFile(tsconfigPath, path_1.dirname(tsconfigPath));
        const host = ts.createCompilerHost(parsed.options, true);
        // We need to overwrite the host "readFile" method, as we want the TypeScript
        // program to be based on the file contents in the virtual file tree. Otherwise
        // if we run the migration for multiple tsconfig files which have intersecting
        // source files, it can end up updating query definitions multiple times.
        host.readFile = fileName => {
            const buffer = tree.read(path_1.relative(basePath, fileName));
            return buffer ? buffer.toString() : undefined;
        };
        const program = ts.createProgram(parsed.fileNames, parsed.options, host);
        const typeChecker = program.getTypeChecker();
        const printer = ts.createPrinter();
        const sourceFiles = program.getSourceFiles().filter(f => !f.isDeclarationFile && !program.isSourceFileFromExternalLibrary(f));
        sourceFiles.forEach(sourceFile => {
            const rendererImport = util_1.findCoreImport(sourceFile, 'Renderer');
            // If there are no imports for the `Renderer`, we can exit early.
            if (!rendererImport) {
                return;
            }
            const { typedNodes, methodCalls, forwardRefs } = util_1.findRendererReferences(sourceFile, typeChecker, rendererImport);
            const update = tree.beginUpdate(path_1.relative(basePath, sourceFile.fileName));
            const helpersToAdd = new Set();
            // Change the `Renderer` import to `Renderer2`.
            update.remove(rendererImport.getStart(), rendererImport.getWidth());
            update.insertRight(rendererImport.getStart(), printer.printNode(ts.EmitHint.Unspecified, migration_1.replaceImport(rendererImport, 'Renderer', 'Renderer2'), sourceFile));
            // Change the method parameter and property types to `Renderer2`.
            typedNodes.forEach(node => {
                const type = node.type;
                if (type) {
                    update.remove(type.getStart(), type.getWidth());
                    update.insertRight(type.getStart(), 'Renderer2');
                }
            });
            // Change all identifiers inside `forwardRef` referring to the `Renderer`.
            forwardRefs.forEach(identifier => {
                update.remove(identifier.getStart(), identifier.getWidth());
                update.insertRight(identifier.getStart(), 'Renderer2');
            });
            // Migrate all of the method calls.
            methodCalls.forEach(call => {
                const { node, requiredHelpers } = migration_1.migrateExpression(call, typeChecker);
                if (node) {
                    // If we migrated the node to a new expression, replace only the call expression.
                    update.remove(call.getStart(), call.getWidth());
                    update.insertRight(call.getStart(), printer.printNode(ts.EmitHint.Unspecified, node, sourceFile));
                }
                else if (call.parent && ts.isExpressionStatement(call.parent)) {
                    // Otherwise if the call is inside an expression statement, drop the entire statement.
                    // This takes care of any trailing semicolons. We only need to drop nodes for cases like
                    // `setBindingDebugInfo` which have been noop for a while so they can be removed safely.
                    update.remove(call.parent.getStart(), call.parent.getWidth());
                }
                if (requiredHelpers) {
                    requiredHelpers.forEach(helperName => helpersToAdd.add(helperName));
                }
            });
            // Some of the methods can't be mapped directly to `Renderer2` and need extra logic around them.
            // The safest way to do so is to declare helper functions similar to the ones emitted by TS
            // which encapsulate the extra "glue" logic. We should only emit these functions once per file.
            helpersToAdd.forEach(helperName => {
                update.insertLeft(sourceFile.endOfFileToken.getStart(), helpers_1.getHelper(helperName, sourceFile, printer));
            });
            tree.commitUpdate(update);
        });
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NjaGVtYXRpY3MvbWlncmF0aW9ucy9yZW5kZXJlci10by1yZW5kZXJlcjIvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7SUFFSCwyREFBNkY7SUFDN0YsK0JBQXVDO0lBQ3ZDLGlDQUFpQztJQUVqQyxrR0FBMkU7SUFDM0UsNkZBQXdFO0lBRXhFLCtGQUFvRDtJQUNwRCxtR0FBNkQ7SUFDN0QseUZBQThEO0lBRzlEOzs7T0FHRztJQUNIO1FBQ0UsT0FBTyxDQUFDLElBQVUsRUFBRSxPQUF5QixFQUFFLEVBQUU7WUFDL0MsTUFBTSxFQUFDLFVBQVUsRUFBRSxTQUFTLEVBQUMsR0FBRyxnREFBdUIsQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUM5RCxNQUFNLFFBQVEsR0FBRyxPQUFPLENBQUMsR0FBRyxFQUFFLENBQUM7WUFDL0IsTUFBTSxRQUFRLEdBQUcsQ0FBQyxHQUFHLFVBQVUsRUFBRSxHQUFHLFNBQVMsQ0FBQyxDQUFDO1lBQy9DLE1BQU0sTUFBTSxHQUFHLE9BQU8sQ0FBQyxNQUFNLENBQUM7WUFFOUIsTUFBTSxDQUFDLElBQUksQ0FBQywrQ0FBK0MsQ0FBQyxDQUFDO1lBQzdELE1BQU0sQ0FBQyxJQUFJLENBQUMsNkRBQTZELENBQUMsQ0FBQztZQUMzRSxNQUFNLENBQUMsSUFBSSxDQUFDLG1DQUFtQyxDQUFDLENBQUM7WUFFakQsSUFBSSxDQUFDLFFBQVEsQ0FBQyxNQUFNLEVBQUU7Z0JBQ3BCLE1BQU0sSUFBSSxnQ0FBbUIsQ0FDekIsZ0ZBQWdGLENBQUMsQ0FBQzthQUN2RjtZQUVELEtBQUssTUFBTSxZQUFZLElBQUksUUFBUSxFQUFFO2dCQUNuQywrQkFBK0IsQ0FBQyxJQUFJLEVBQUUsWUFBWSxFQUFFLFFBQVEsQ0FBQyxDQUFDO2FBQy9EO1FBQ0gsQ0FB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QUMsVUFBVSxDQUFDLENBQUMsQ0FBQztpQkFDckU7WUFDSCxDQUFDLENBQUMsQ0FBQztZQUVILGdHQUFnRztZQUNoRywyRkFBMkY7WUFDM0YsK0ZBQStGO1lBQy9GLFlBQVksQ0FBQyxPQUFPLENBQUMsVUFBVSxDQUFDLEVBQUU7Z0JBQ2hDLE1BQU0sQ0FBQyxVQUFVLENBQ2IsVUFBVSxDQUFDLGNBQWMsQ0FBQyxRQUFRLEVBQUUsRUFBRSxtQkFBUyxDQUFDLFVBQVUsRUFBRSxVQUFVLEVBQUUsT0FBTyxDQUFDLENBQUMsQ0FBQztZQUN4RixDQUFDLENBQUMsQ0FBQztZQUVILElBQUksQ0FBQyxZQUFZLENBQUMsTUFBTSxDQUFDLENBQUM7UUFDNUIsQ0FBQyxDQUFDLENBQUM7SUFDTCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1J1bGUsIFNjaGVtYXRpY0NvbnRleHQsIFNjaGVtYXRpY3NFeGNlcHRpb24sIFRyZWV9IGZyb20gJ0Bhbmd1bGFyLWRldmtpdC9zY2hlbWF0aWNzJztcbmltcG9ydCB7ZGlybmFtZSwgcmVsYXRpdmV9IGZyb20gJ3BhdGgnO1xuaW1wb3J0ICogYXMgdHMgZnJvbSAndHlwZXNjcmlwdCc7XG5cbmltcG9ydCB7Z2V0UHJvamVjdFRzQ29uZmlnUGF0aHN9IGZyb20gJy4uLy4uL3V0aWxzL3Byb2plY3RfdHNjb25maWdfcGF0aHMnO1xuaW1wb3J0IHtwYXJzZVRzY29uZmlnRmlsZX0gZnJvbSAnLi4vLi4vdXRpbHMvdHlwZXNjcmlwdC9wYXJzZV90c2NvbmZpZyc7XG5cbmltcG9ydCB7SGVscGVyRnVuY3Rpb24sIGdldEhlbHBlcn0gZnJvbSAnLi9oZWxwZXJzJztcbmltcG9ydCB7bWlncmF0ZUV4cHJlc3Npb24sIHJlcGxhY2VJbXBvcnR9IGZyb20gJy4vbWlncmF0aW9uJztcbmltcG9ydCB7ZmluZENvcmVJbXBvcnQsIGZpbmRSZW5kZXJlclJlZmVyZW5jZXN9IGZyb20gJy4vdXRpbCc7XG5cblxuLyoqXG4gKiBNaWdyYXRpb24gdGhhdCBzd2l0Y2hlcyBmcm9tIGBSZW5kZXJlcmAgdG8gYFJlbmRlcmVyMmAuIE1vcmUgaW5mb3JtYXRpb24gb24gaG93IGl0IHdvcmtzOlxuICogaHR0cHM6Ly9oYWNrbWQuYW5ndWxhci5pby9VVHpVWlRuUFJBLWNTYV80bUh5Zll3XG4gKi9cbmV4cG9ydCBkZWZhdWx0IGZ1bmN0aW9uKCk6IFJ1bGUge1xuICByZXR1cm4gKHRyZWU6IFRyZWUsIGNvbnRleHQ6IFNjaGVtYXRpY0NvbnRleHQpID0+IHtcbiAgICBjb25zdCB7YnVpbGRQYXRocywgdGVzdFBhdGhzfSA9IGdldFByb2plY3RUc0NvbmZpZ1BhdGhzKHRyZWUpO1xuICAgIGNvbnN0IGJhc2VQYXRoID0gcHJvY2Vzcy5jd2QoKTtcbiAgICBjb25zdCBhbGxQYXRocyA9IFsuLi5idWlsZFBhdGhzLCAuLi50ZXN0UGF0aHNdO1xuICAgIGNvbnN0IGxvZ2dlciA9IGNvbnRleHQubG9nZ2VyO1xuXG4gICAgbG9nZ2VyLmluZm8oJy0tLS0tLSBSZW5kZXJlciB0byBSZW5kZXJlcjIgTWlncmF0aW9uIC0tLS0tLScpO1xuICAgIGxvZ2dlci5pbmZvKCdBcyBvZiBBbmd1bGFyIDksIHRoZSBSZW5kZXJlciBjbGFzcyBpcyBubyBsb25nZXIgYXZhaWxhYmxlLicpO1xuICAgIGxvZ2dlci5pbmZvKCdSZW5kZXJlcjIgc2hvdWxkIGJlIHVzZWQgaW5zdGVhZC4nKTtcblxuICAgIGlmICghYWxsUGF0aHMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgU2NoZW1hdGljc0V4Y2VwdGlvbihcbiAgICAgICAgICAnQ291bGQgbm90IGZpbmQgYW55IHRzY29uZmlnIGZpbGUuIENhbm5vdCBtaWdyYXRlIFJlbmRlcmVyIHVzYWdlcyB0byBSZW5kZXJlcjIuJyk7XG4gICAgfVxuXG4gICAgZm9yIChjb25zdCB0c2NvbmZpZ1BhdGggb2YgYWxsUGF0aHMpIHtcbiAgICAgIHJ1blJlbmRlcmVyVG9SZW5kZXJlcjJNaWdyYXRpb24odHJlZSwgdHNjb25maWdQYXRoLCBiYXNlUGF0aCk7XG4gICAgfVxuICB9O1xufVxuXG5mdW5jdGlvbiBydW5SZW5kZXJlclRvUmVuZGVyZXIyTWlncmF0aW9uKHRyZWU6IFRyZWUsIHRzY29uZmlnUGF0aDogc3RyaW5nLCBiYXNlUGF0aDogc3RyaW5nKSB7XG4gIGNvbnN0IHBhcnNlZCA9IHBhcnNlVHNjb25maWdGaWxlKHRzY29uZmlnUGF0aCwgZGlybmFtZSh0c2NvbmZpZ1BhdGgpKTtcbiAgY29uc3QgaG9zdCA9IHRzLmNyZWF0ZUNvbXBpbGVySG9zdChwYXJzZWQub3B0aW9ucywgdHJ1ZSk7XG5cbiAgLy8gV2UgbmVlZCB0byBvdmVyd3JpdGUgdGhlIGhvc3QgXCJyZWFkRmlsZVwiIG1ldGhvZCwgYXMgd2Ugd2FudCB0aGUgVHlwZVNjcmlwdFxuICAvLyBwcm9ncmFtIHRvIGJlIGJhc2VkIG9uIHRoZSBmaWxlIGNvbnRlbnRzIGluIHRoZSB2aXJ0dWFsIGZpbGUgdHJlZS4gT3RoZXJ3aXNlXG4gIC8vIGlmIHdlIHJ1biB0aGUgbWlncmF0aW9uIGZvciBtdWx0aXBsZSB0c2NvbmZpZyBmaWxlcyB3aGljaCBoYXZlIGludGVyc2VjdGluZ1xuICAvLyBzb3VyY2UgZmlsZXMsIGl0IGNhbiBlbmQgdXAgdXBkYXRpbmcgcXVlcnkgZGVmaW5pdGlvbnMgbXVsdGlwbGUgdGltZXMuXG4gIGhvc3QucmVhZEZpbGUgPSBmaWxlTmFtZSA9PiB7XG4gICAgY29uc3QgYnVmZmVyID0gdHJlZS5yZWFkKHJlbGF0aXZlKGJhc2VQYXRoLCBmaWxlTmFtZSkpO1xuICAgIHJldHVybiBidWZmZXIgPyBidWZmZXIudG9TdHJpbmcoKSA6IHVuZGVmaW5lZDtcbiAgfTtcblxuICBjb25zdCBwcm9ncmFtID0gdHMuY3JlYXRlUHJvZ3JhbShwYXJzZWQuZmlsZU5hbWVzLCBwYXJzZWQub3B0aW9ucywgaG9zdCk7XG4gIGNvbnN0IHR5cGVDaGVja2VyID0gcHJvZ3JhbS5nZXRUeXBlQ2hlY2tlcigpO1xuICBjb25zdCBwcmludGVyID0gdHMuY3JlYXRlUHJpbnRlcigpO1xuICBjb25zdCBzb3VyY2VGaWxlcyA9IHByb2dyYW0uZ2V0U291cmNlRmlsZXMoKS5maWx0ZXIoXG4gICAgICBmID0+ICFmLmlzRGVjbGFyYXRpb25GaWxlICYmICFwcm9ncmFtLmlzU291cmNlRmlsZUZyb21FeHRlcm5hbExpYnJhcnkoZikpO1xuXG4gIHNvdXJjZUZpbGVzLmZvckVhY2goc291cmNlRmlsZSA9PiB7XG4gICAgY29uc3QgcmVuZGVyZXJJbXBvcnQgPSBmaW5kQ29yZUltcG9ydChzb3VyY2VGaWxlLCAnUmVuZGVyZXInKTtcblxuICAgIC8vIElmIHRoZXJlIGFyZSBubyBpbXBvcnRzIGZvciB0aGUgYFJlbmRlcmVyYCwgd2UgY2FuIGV4aXQgZWFybHkuXG4gICAgaWYgKCFyZW5kZXJlckltcG9ydCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnN0IHt0eXBlZE5vZGVzLCBtZXRob2RDYWxscywgZm9yd2FyZFJlZnN9ID1cbiAgICAgICAgZmluZFJlbmRlcmVyUmVmZXJlbmNlcyhzb3VyY2VGaWxlLCB0eXBlQ2hlY2tlciwgcmVuZGVyZXJJbXBvcnQpO1xuICAgIGNvbnN0IHVwZGF0ZSA9IHRyZWUuYmVnaW5VcGRhdGUocmVsYXRpdmUoYmFzZVBhdGgsIHNvdXJjZUZpbGUuZmlsZU5hbWUpKTtcbiAgICBjb25zdCBoZWxwZXJzVG9BZGQgPSBuZXcgU2V0PEhlbHBlckZ1bmN0aW9uPigpO1xuXG4gICAgLy8gQ2hhbmdlIHRoZSBgUmVuZGVyZXJgIGltcG9ydCB0byBgUmVuZGVyZXIyYC5cbiAgICB1cGRhdGUucmVtb3ZlKHJlbmRlcmVySW1wb3J0LmdldFN0YXJ0KCksIHJlbmRlcmVySW1wb3J0LmdldFdpZHRoKCkpO1xuICAgIHVwZGF0ZS5pbnNlcnRSaWdodChcbiAgICAgICAgcmVuZGVyZXJJbXBvcnQuZ2V0U3RhcnQoKSxcbiAgICAgICAgcHJpbnRlci5wcmludE5vZGUoXG4gICAgICAgICAgICB0cy5FbWl0SGludC5VbnNwZWNpZmllZCwgcmVwbGFjZUltcG9ydChyZW5kZXJlckltcG9ydCwgJ1JlbmRlcmVyJywgJ1JlbmRlcmVyMicpLFxuICAgICAgICAgICAgc291cmNlRmlsZSkpO1xuXG4gICAgLy8gQ2hhbmdlIHRoZSBtZXRob2QgcGFyYW1ldGVyIGFuZCBwcm9wZXJ0eSB0eXBlcyB0byBgUmVuZGVyZXIyYC5cbiAgICB0eXBlZE5vZGVzLmZvckVhY2gobm9kZSA9PiB7XG4gICAgICBjb25zdCB0eXBlID0gbm9kZS50eXBlO1xuXG4gICAgICBpZiAodHlwZSkge1xuICAgICAgICB1cGRhdGUucmVtb3ZlKHR5cGUuZ2V0U3RhcnQoKSwgdHlwZS5nZXRXaWR0aCgpKTtcbiAgICAgICAgdXBkYXRlLmluc2VydFJpZ2h0KHR5cGUuZ2V0U3RhcnQoKSwgJ1JlbmRlcmVyMicpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgLy8gQ2hhbmdlIGFsbCBpZGVudGlmaWVycyBpbnNpZGUgYGZvcndhcmRSZWZgIHJlZmVycmluZyB0byB0aGUgYFJlbmRlcmVyYC5cbiAgICBmb3J3YXJkUmVmcy5mb3JFYWNoKGlkZW50aWZpZXIgPT4ge1xuICAgICAgdXBkYXRlLnJlbW92ZShpZGVudGlmaWVyLmdldFN0YXJ0KCksIGlkZW50aWZpZXIuZ2V0V2lkdGgoKSk7XG4gICAgICB1cGRhdGUuaW5zZXJ0UmlnaHQoaWRlbnRpZmllci5nZXRTdGFydCgpLCAnUmVuZGVyZXIyJyk7XG4gICAgfSk7XG5cbiAgICAvLyBNaWdyYXRlIGFsbCBvZiB0aGUgbWV0aG9kIGNhbGxzLlxuICAgIG1ldGhvZENhbGxzLmZvckVhY2goY2FsbCA9PiB7XG4gICAgICBjb25zdCB7bm9kZSwgcmVxdWlyZWRIZWxwZXJzfSA9IG1pZ3JhdGVFeHByZXNzaW9uKGNhbGwsIHR5cGVDaGVja2VyKTtcblxuICAgICAgaWYgKG5vZGUpIHtcbiAgICAgICAgLy8gSWYgd2UgbWlncmF0ZWQgdGhlIG5vZGUgdG8gYSBuZXcgZXhwcmVzc2lvbiwgcmVwbGFjZSBvbmx5IHRoZSBjYWxsIGV4cHJlc3Npb24uXG4gICAgICAgIHVwZGF0ZS5yZW1vdmUoY2FsbC5nZXRTdGFydCgpLCBjYWxsLmdldFdpZHRoKCkpO1xuICAgICAgICB1cGRhdGUuaW5zZXJ0UmlnaHQoXG4gICAgICAgICAgICBjYWxsLmdldFN0YXJ0KCksIHByaW50ZXIucHJpbnROb2RlKHRzLkVtaXRIaW50LlVuc3BlY2lmaWVkLCBub2RlLCBzb3VyY2VGaWxlKSk7XG4gICAgICB9IGVsc2UgaWYgKGNhbGwucGFyZW50ICYmIHRzLmlzRXhwcmVzc2lvblN0YXRlbWVudChjYWxsLnBhcmVudCkpIHtcbiAgICAgICAgLy8gT3RoZXJ3aXNlIGlmIHRoZSBjYWxsIGlzIGluc2lkZSBhbiBleHByZXNzaW9uIHN0YXRlbWVudCwgZHJvcCB0aGUgZW50aXJlIHN0YXRlbWVudC5cbiAgICAgICAgLy8gVGhpcyB0YWtlcyBjYXJlIG9mIGFueSB0cmFpbGluZyBzZW1pY29sb25zLiBXZSBvbmx5IG5lZWQgdG8gZHJvcCBub2RlcyBmb3IgY2FzZXMgbGlrZVxuICAgICAgICAvLyBgc2V0QmluZGluZ0RlYnVnSW5mb2Agd2hpY2ggaGF2ZSBiZWVuIG5vb3AgZm9yIGEgd2hpbGUgc28gdGhleSBjYW4gYmUgcmVtb3ZlZCBzYWZlbHkuXG4gICAgICAgIHVwZGF0ZS5yZW1vdmUoY2FsbC5wYXJlbnQuZ2V0U3RhcnQoKSwgY2FsbC5wYXJlbnQuZ2V0V2lkdGgoKSk7XG4gICAgICB9XG5cbiAgICAgIGlmIChyZXF1aXJlZEhlbHBlcnMpIHtcbiAgICAgICAgcmVxdWlyZWRIZWxwZXJzLmZvckVhY2goaGVscGVyTmFtZSA9PiBoZWxwZXJzVG9BZGQuYWRkKGhlbHBlck5hbWUpKTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIC8vIFNvbWUgb2YgdGhlIG1ldGhvZHMgY2FuJ3QgYmUgbWFwcGVkIGRpcmVjdGx5IHRvIGBSZW5kZXJlcjJgIGFuZCBuZWVkIGV4dHJhIGxvZ2ljIGFyb3VuZCB0aGVtLlxuICAgIC8vIFRoZSBzYWZlc3Qgd2F5IHRvIGRvIHNvIGlzIHRvIGRlY2xhcmUgaGVscGVyIGZ1bmN0aW9ucyBzaW1pbGFyIHRvIHRoZSBvbmVzIGVtaXR0ZWQgYnkgVFNcbiAgICAvLyB3aGljaCBlbmNhcHN1bGF0ZSB0aGUgZXh0cmEgXCJnbHVlXCIgbG9naWMuIFdlIHNob3VsZCBvbmx5IGVtaXQgdGhlc2UgZnVuY3Rpb25zIG9uY2UgcGVyIGZpbGUuXG4gICAgaGVscGVyc1RvQWRkLmZvckVhY2goaGVscGVyTmFtZSA9PiB7XG4gICAgICB1cGRhdGUuaW5zZXJ0TGVmdChcbiAgICAgICAgICBzb3VyY2VGaWxlLmVuZE9mRmlsZVRva2VuLmdldFN0YXJ0KCksIGdldEhlbHBlcihoZWxwZXJOYW1lLCBzb3VyY2VGaWxlLCBwcmludGVyKSk7XG4gICAgfSk7XG5cbiAgICB0cmVlLmNvbW1pdFVwZGF0ZSh1cGRhdGUpO1xuICB9KTtcbn1cbiJdfQ==