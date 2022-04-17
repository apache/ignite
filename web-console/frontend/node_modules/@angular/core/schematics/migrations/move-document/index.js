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
        define("@angular/core/schematics/migrations/move-document", ["require", "exports", "@angular-devkit/schematics", "path", "typescript", "@angular/core/schematics/utils/project_tsconfig_paths", "@angular/core/schematics/utils/typescript/parse_tsconfig", "@angular/core/schematics/migrations/move-document/document_import_visitor", "@angular/core/schematics/migrations/move-document/move-import"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const schematics_1 = require("@angular-devkit/schematics");
    const path_1 = require("path");
    const ts = require("typescript");
    const project_tsconfig_paths_1 = require("@angular/core/schematics/utils/project_tsconfig_paths");
    const parse_tsconfig_1 = require("@angular/core/schematics/utils/typescript/parse_tsconfig");
    const document_import_visitor_1 = require("@angular/core/schematics/migrations/move-document/document_import_visitor");
    const move_import_1 = require("@angular/core/schematics/migrations/move-document/move-import");
    /** Entry point for the V8 move-document migration. */
    function default_1() {
        return (tree) => {
            const { buildPaths, testPaths } = project_tsconfig_paths_1.getProjectTsConfigPaths(tree);
            const basePath = process.cwd();
            if (!buildPaths.length && !testPaths.length) {
                throw new schematics_1.SchematicsException(`Could not find any tsconfig file. Cannot migrate DOCUMENT 
          to new import source.`);
            }
            for (const tsconfigPath of [...buildPaths, ...testPaths]) {
                runMoveDocumentMigration(tree, tsconfigPath, basePath);
            }
        };
    }
    exports.default = default_1;
    /**
     * Runs the DOCUMENT InjectionToken import migration for the given TypeScript project. The
     * schematic analyzes the imports within the project and moves the deprecated symbol to the
     * new import source.
     */
    function runMoveDocumentMigration(tree, tsconfigPath, basePath) {
        const parsed = parse_tsconfig_1.parseTsconfigFile(tsconfigPath, path_1.dirname(tsconfigPath));
        const host = ts.createCompilerHost(parsed.options, true);
        // We need to overwrite the host "readFile" method, as we want the TypeScript
        // program to be based on the file contents in the virtual file tree. Otherwise
        // if we run the migration for multiple tsconfig files which have intersecting
        // source files, it can end up updating query definitions multiple times.
        host.readFile = fileName => {
            const buffer = tree.read(path_1.relative(basePath, fileName));
            // Strip BOM as otherwise TSC methods (Ex: getWidth) will return an offset which
            // which breaks the CLI UpdateRecorder.
            // See: https://github.com/angular/angular/pull/30719
            return buffer ? buffer.toString().replace(/^\uFEFF/, '') : undefined;
        };
        const program = ts.createProgram(parsed.fileNames, parsed.options, host);
        const typeChecker = program.getTypeChecker();
        const visitor = new document_import_visitor_1.DocumentImportVisitor(typeChecker);
        const sourceFiles = program.getSourceFiles().filter(f => !f.isDeclarationFile && !program.isSourceFileFromExternalLibrary(f));
        // Analyze source files by finding imports.
        sourceFiles.forEach(sourceFile => visitor.visitNode(sourceFile));
        const { importsMap } = visitor;
        // Walk through all source files that contain resolved queries and update
        // the source files if needed. Note that we need to update multiple queries
        // within a source file within the same recorder in order to not throw off
        // the TypeScript node offsets.
        importsMap.forEach((resolvedImport, sourceFile) => {
            const { platformBrowserImport, commonImport, documentElement } = resolvedImport;
            if (!documentElement || !platformBrowserImport) {
                return;
            }
            const update = tree.beginUpdate(path_1.relative(basePath, sourceFile.fileName));
            const platformBrowserDeclaration = platformBrowserImport.parent.parent;
            const newPlatformBrowserText = move_import_1.removeFromImport(platformBrowserImport, sourceFile, document_import_visitor_1.DOCUMENT_TOKEN_NAME);
            const newCommonText = commonImport ?
                move_import_1.addToImport(commonImport, sourceFile, documentElement.name, documentElement.propertyName) :
                move_import_1.createImport(document_import_visitor_1.COMMON_IMPORT, sourceFile, documentElement.name, documentElement.propertyName);
            // Replace the existing query decorator call expression with the updated
            // call expression node.
            update.remove(platformBrowserDeclaration.getStart(), platformBrowserDeclaration.getWidth());
            update.insertRight(platformBrowserDeclaration.getStart(), newPlatformBrowserText);
            if (commonImport) {
                const commonDeclaration = commonImport.parent.parent;
                update.remove(commonDeclaration.getStart(), commonDeclaration.getWidth());
                update.insertRight(commonDeclaration.getStart(), newCommonText);
            }
            else {
                update.insertRight(platformBrowserDeclaration.getStart(), newCommonText);
            }
            tree.commitUpdate(update);
        });
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NjaGVtYXRpY3MvbWlncmF0aW9ucy9tb3ZlLWRvY3VtZW50L2luZGV4LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgsMkRBQTJFO0lBQzNFLCtCQUF1QztJQUN2QyxpQ0FBaUM7SUFFakMsa0dBQTJFO0lBQzNFLDZGQUF3RTtJQUN4RSx1SEFBNEg7SUFDNUgsK0ZBQTBFO0lBRzFFLHNEQUFzRDtJQUN0RDtRQUNFLE9BQU8sQ0FBQyxJQUFVLEVBQUUsRUFBRTtZQUNwQixNQUFNLEVBQUMsVUFBVSxFQUFFLFNBQVMsRUFBQyxHQUFHLGdEQUF1QixDQUFDLElBQUksQ0FBQyxDQUFDO1lBQzlELE1BQU0sUUFBUSxHQUFHLE9BQU8sQ0FBQyxHQUFHLEVBQUUsQ0FBQztZQUUvQixJQUFJLENBQUMsVUFBVSxDQUFDLE1BQU0sSUFBSSxDQUFDLFNBQVMsQ0FBQyxNQUFNLEVBQUU7Z0JBQzNDLE1BQU0sSUFBSSxnQ0FBbUIsQ0FBQztnQ0FDSixDQUFDLENBQUM7YUFDN0I7WUFFRCxLQUFLLE1BQU0sWUFBWSxJQUFJLENBQUMsR0FBRyxVQUFVLEVBQUUsR0FBRyxTQUFTLENBQUMsRUFBRTtnQkFDeEQsd0JBQXdCLENBQUMsSUFBSSxFQUFFLFlBQVksRUFBRSxRQUFRLENBQUMsQ0FBQzthQUN4RDtRQUNILENBQUMsQ0FBQztJQUNKLENBQUM7SUFkRCw0QkFjQztJQUVEOzs7O09BSUc7SUFDSCxTQUFTLHdCQUF3QixDQUFDLElBQVUsRUFBRSxZQUFvQixFQUFFLFFBQWdCO1FBQ2xGLE1BQU0sTUFBTSxHQUFHLGtDQUFpQixDQUFDLFlBQVksRUFBRSxjQUFPLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQztRQUN0RSxNQUFNLElBQUksR0FBRyxFQUFFLENBQUMsa0JBQWtCLENBQUMsTUFB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LGlCQUFpQixDQUFDLFFBQVEsRUFBRSxFQUFFLGlCQUFpQixDQUFDLFFBQVEsRUFBRSxDQUFDLENBQUM7Z0JBQzFFLE1BQU0sQ0FBQyxXQUFXLENBQUMsaUJBQWlCLENBQUMsUUFBUSxFQUFFLEVBQUUsYUFBYSxDQUFDLENBQUM7YUFDakU7aUJBQU07Z0JBQ0wsTUFBTSxDQUFDLFdBQVcsQ0FBQywwQkFBMEIsQ0FBQyxRQUFRLEVBQUUsRUFBRSxhQUFhLENBQUMsQ0FBQzthQUMxRTtZQUVELElBQUksQ0FBQyxZQUFZLENBQUMsTUFBTSxDQUFDLENBQUM7UUFDNUIsQ0FBQyxDQUFDLENBQUM7SUFDTCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1J1bGUsIFNjaGVtYXRpY3NFeGNlcHRpb24sIFRyZWV9IGZyb20gJ0Bhbmd1bGFyLWRldmtpdC9zY2hlbWF0aWNzJztcbmltcG9ydCB7ZGlybmFtZSwgcmVsYXRpdmV9IGZyb20gJ3BhdGgnO1xuaW1wb3J0ICogYXMgdHMgZnJvbSAndHlwZXNjcmlwdCc7XG5cbmltcG9ydCB7Z2V0UHJvamVjdFRzQ29uZmlnUGF0aHN9IGZyb20gJy4uLy4uL3V0aWxzL3Byb2plY3RfdHNjb25maWdfcGF0aHMnO1xuaW1wb3J0IHtwYXJzZVRzY29uZmlnRmlsZX0gZnJvbSAnLi4vLi4vdXRpbHMvdHlwZXNjcmlwdC9wYXJzZV90c2NvbmZpZyc7XG5pbXBvcnQge0NPTU1PTl9JTVBPUlQsIERPQ1VNRU5UX1RPS0VOX05BTUUsIERvY3VtZW50SW1wb3J0VmlzaXRvciwgUmVzb2x2ZWREb2N1bWVudEltcG9ydH0gZnJvbSAnLi9kb2N1bWVudF9pbXBvcnRfdmlzaXRvcic7XG5pbXBvcnQge2FkZFRvSW1wb3J0LCBjcmVhdGVJbXBvcnQsIHJlbW92ZUZyb21JbXBvcnR9IGZyb20gJy4vbW92ZS1pbXBvcnQnO1xuXG5cbi8qKiBFbnRyeSBwb2ludCBmb3IgdGhlIFY4IG1vdmUtZG9jdW1lbnQgbWlncmF0aW9uLiAqL1xuZXhwb3J0IGRlZmF1bHQgZnVuY3Rpb24oKTogUnVsZSB7XG4gIHJldHVybiAodHJlZTogVHJlZSkgPT4ge1xuICAgIGNvbnN0IHtidWlsZFBhdGhzLCB0ZXN0UGF0aHN9ID0gZ2V0UHJvamVjdFRzQ29uZmlnUGF0aHModHJlZSk7XG4gICAgY29uc3QgYmFzZVBhdGggPSBwcm9jZXNzLmN3ZCgpO1xuXG4gICAgaWYgKCFidWlsZFBhdGhzLmxlbmd0aCAmJiAhdGVzdFBhdGhzLmxlbmd0aCkge1xuICAgICAgdGhyb3cgbmV3IFNjaGVtYXRpY3NFeGNlcHRpb24oYENvdWxkIG5vdCBmaW5kIGFueSB0c2NvbmZpZyBmaWxlLiBDYW5ub3QgbWlncmF0ZSBET0NVTUVOVCBcbiAgICAgICAgICB0byBuZXcgaW1wb3J0IHNvdXJjZS5gKTtcbiAgICB9XG5cbiAgICBmb3IgKGNvbnN0IHRzY29uZmlnUGF0aCBvZiBbLi4uYnVpbGRQYXRocywgLi4udGVzdFBhdGhzXSkge1xuICAgICAgcnVuTW92ZURvY3VtZW50TWlncmF0aW9uKHRyZWUsIHRzY29uZmlnUGF0aCwgYmFzZVBhdGgpO1xuICAgIH1cbiAgfTtcbn1cblxuLyoqXG4gKiBSdW5zIHRoZSBET0NVTUVOVCBJbmplY3Rpb25Ub2tlbiBpbXBvcnQgbWlncmF0aW9uIGZvciB0aGUgZ2l2ZW4gVHlwZVNjcmlwdCBwcm9qZWN0LiBUaGVcbiAqIHNjaGVtYXRpYyBhbmFseXplcyB0aGUgaW1wb3J0cyB3aXRoaW4gdGhlIHByb2plY3QgYW5kIG1vdmVzIHRoZSBkZXByZWNhdGVkIHN5bWJvbCB0byB0aGVcbiAqIG5ldyBpbXBvcnQgc291cmNlLlxuICovXG5mdW5jdGlvbiBydW5Nb3ZlRG9jdW1lbnRNaWdyYXRpb24odHJlZTogVHJlZSwgdHNjb25maWdQYXRoOiBzdHJpbmcsIGJhc2VQYXRoOiBzdHJpbmcpIHtcbiAgY29uc3QgcGFyc2VkID0gcGFyc2VUc2NvbmZpZ0ZpbGUodHNjb25maWdQYXRoLCBkaXJuYW1lKHRzY29uZmlnUGF0aCkpO1xuICBjb25zdCBob3N0ID0gdHMuY3JlYXRlQ29tcGlsZXJIb3N0KHBhcnNlZC5vcHRpb25zLCB0cnVlKTtcblxuICAvLyBXZSBuZWVkIHRvIG92ZXJ3cml0ZSB0aGUgaG9zdCBcInJlYWRGaWxlXCIgbWV0aG9kLCBhcyB3ZSB3YW50IHRoZSBUeXBlU2NyaXB0XG4gIC8vIHByb2dyYW0gdG8gYmUgYmFzZWQgb24gdGhlIGZpbGUgY29udGVudHMgaW4gdGhlIHZpcnR1YWwgZmlsZSB0cmVlLiBPdGhlcndpc2VcbiAgLy8gaWYgd2UgcnVuIHRoZSBtaWdyYXRpb24gZm9yIG11bHRpcGxlIHRzY29uZmlnIGZpbGVzIHdoaWNoIGhhdmUgaW50ZXJzZWN0aW5nXG4gIC8vIHNvdXJjZSBmaWxlcywgaXQgY2FuIGVuZCB1cCB1cGRhdGluZyBxdWVyeSBkZWZpbml0aW9ucyBtdWx0aXBsZSB0aW1lcy5cbiAgaG9zdC5yZWFkRmlsZSA9IGZpbGVOYW1lID0+IHtcbiAgICBjb25zdCBidWZmZXIgPSB0cmVlLnJlYWQocmVsYXRpdmUoYmFzZVBhdGgsIGZpbGVOYW1lKSk7XG4gICAgLy8gU3RyaXAgQk9NIGFzIG90aGVyd2lzZSBUU0MgbWV0aG9kcyAoRXg6IGdldFdpZHRoKSB3aWxsIHJldHVybiBhbiBvZmZzZXQgd2hpY2hcbiAgICAvLyB3aGljaCBicmVha3MgdGhlIENMSSBVcGRhdGVSZWNvcmRlci5cbiAgICAvLyBTZWU6IGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIvcHVsbC8zMDcxOVxuICAgIHJldHVybiBidWZmZXIgPyBidWZmZXIudG9TdHJpbmcoKS5yZXBsYWNlKC9eXFx1RkVGRi8sICcnKSA6IHVuZGVmaW5lZDtcbiAgfTtcblxuICBjb25zdCBwcm9ncmFtID0gdHMuY3JlYXRlUHJvZ3JhbShwYXJzZWQuZmlsZU5hbWVzLCBwYXJzZWQub3B0aW9ucywgaG9zdCk7XG4gIGNvbnN0IHR5cGVDaGVja2VyID0gcHJvZ3JhbS5nZXRUeXBlQ2hlY2tlcigpO1xuICBjb25zdCB2aXNpdG9yID0gbmV3IERvY3VtZW50SW1wb3J0VmlzaXRvcih0eXBlQ2hlY2tlcik7XG4gIGNvbnN0IHNvdXJjZUZpbGVzID0gcHJvZ3JhbS5nZXRTb3VyY2VGaWxlcygpLmZpbHRlcihcbiAgICAgIGYgPT4gIWYuaXNEZWNsYXJhdGlvbkZpbGUgJiYgIXByb2dyYW0uaXNTb3VyY2VGaWxlRnJvbUV4dGVybmFsTGlicmFyeShmKSk7XG5cbiAgLy8gQW5hbHl6ZSBzb3VyY2UgZmlsZXMgYnkgZmluZGluZyBpbXBvcnRzLlxuICBzb3VyY2VGaWxlcy5mb3JFYWNoKHNvdXJjZUZpbGUgPT4gdmlzaXRvci52aXNpdE5vZGUoc291cmNlRmlsZSkpO1xuXG4gIGNvbnN0IHtpbXBvcnRzTWFwfSA9IHZpc2l0b3I7XG5cbiAgLy8gV2FsayB0aHJvdWdoIGFsbCBzb3VyY2UgZmlsZXMgdGhhdCBjb250YWluIHJlc29sdmVkIHF1ZXJpZXMgYW5kIHVwZGF0ZVxuICAvLyB0aGUgc291cmNlIGZpbGVzIGlmIG5lZWRlZC4gTm90ZSB0aGF0IHdlIG5lZWQgdG8gdXBkYXRlIG11bHRpcGxlIHF1ZXJpZXNcbiAgLy8gd2l0aGluIGEgc291cmNlIGZpbGUgd2l0aGluIHRoZSBzYW1lIHJlY29yZGVyIGluIG9yZGVyIHRvIG5vdCB0aHJvdyBvZmZcbiAgLy8gdGhlIFR5cGVTY3JpcHQgbm9kZSBvZmZzZXRzLlxuICBpbXBvcnRzTWFwLmZvckVhY2goKHJlc29sdmVkSW1wb3J0OiBSZXNvbHZlZERvY3VtZW50SW1wb3J0LCBzb3VyY2VGaWxlOiB0cy5Tb3VyY2VGaWxlKSA9PiB7XG4gICAgY29uc3Qge3BsYXRmb3JtQnJvd3NlckltcG9ydCwgY29tbW9uSW1wb3J0LCBkb2N1bWVudEVsZW1lbnR9ID0gcmVzb2x2ZWRJbXBvcnQ7XG4gICAgaWYgKCFkb2N1bWVudEVsZW1lbnQgfHwgIXBsYXRmb3JtQnJvd3NlckltcG9ydCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBjb25zdCB1cGRhdGUgPSB0cmVlLmJlZ2luVXBkYXRlKHJlbGF0aXZlKGJhc2VQYXRoLCBzb3VyY2VGaWxlLmZpbGVOYW1lKSk7XG5cbiAgICBjb25zdCBwbGF0Zm9ybUJyb3dzZXJEZWNsYXJhdGlvbiA9IHBsYXRmb3JtQnJvd3NlckltcG9ydC5wYXJlbnQucGFyZW50O1xuICAgIGNvbnN0IG5ld1BsYXRmb3JtQnJvd3NlclRleHQgPVxuICAgICAgICByZW1vdmVGcm9tSW1wb3J0KHBsYXRmb3JtQnJvd3NlckltcG9ydCwgc291cmNlRmlsZSwgRE9DVU1FTlRfVE9LRU5fTkFNRSk7XG4gICAgY29uc3QgbmV3Q29tbW9uVGV4dCA9IGNvbW1vbkltcG9ydCA/XG4gICAgICAgIGFkZFRvSW1wb3J0KGNvbW1vbkltcG9ydCwgc291cmNlRmlsZSwgZG9jdW1lbnRFbGVtZW50Lm5hbWUsIGRvY3VtZW50RWxlbWVudC5wcm9wZXJ0eU5hbWUpIDpcbiAgICAgICAgY3JlYXRlSW1wb3J0KENPTU1PTl9JTVBPUlQsIHNvdXJjZUZpbGUsIGRvY3VtZW50RWxlbWVudC5uYW1lLCBkb2N1bWVudEVsZW1lbnQucHJvcGVydHlOYW1lKTtcblxuICAgIC8vIFJlcGxhY2UgdGhlIGV4aXN0aW5nIHF1ZXJ5IGRlY29yYXRvciBjYWxsIGV4cHJlc3Npb24gd2l0aCB0aGUgdXBkYXRlZFxuICAgIC8vIGNhbGwgZXhwcmVzc2lvbiBub2RlLlxuICAgIHVwZGF0ZS5yZW1vdmUocGxhdGZvcm1Ccm93c2VyRGVjbGFyYXRpb24uZ2V0U3RhcnQoKSwgcGxhdGZvcm1Ccm93c2VyRGVjbGFyYXRpb24uZ2V0V2lkdGgoKSk7XG4gICAgdXBkYXRlLmluc2VydFJpZ2h0KHBsYXRmb3JtQnJvd3NlckRlY2xhcmF0aW9uLmdldFN0YXJ0KCksIG5ld1BsYXRmb3JtQnJvd3NlclRleHQpO1xuXG4gICAgaWYgKGNvbW1vbkltcG9ydCkge1xuICAgICAgY29uc3QgY29tbW9uRGVjbGFyYXRpb24gPSBjb21tb25JbXBvcnQucGFyZW50LnBhcmVudDtcbiAgICAgIHVwZGF0ZS5yZW1vdmUoY29tbW9uRGVjbGFyYXRpb24uZ2V0U3RhcnQoKSwgY29tbW9uRGVjbGFyYXRpb24uZ2V0V2lkdGgoKSk7XG4gICAgICB1cGRhdGUuaW5zZXJ0UmlnaHQoY29tbW9uRGVjbGFyYXRpb24uZ2V0U3RhcnQoKSwgbmV3Q29tbW9uVGV4dCk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHVwZGF0ZS5pbnNlcnRSaWdodChwbGF0Zm9ybUJyb3dzZXJEZWNsYXJhdGlvbi5nZXRTdGFydCgpLCBuZXdDb21tb25UZXh0KTtcbiAgICB9XG5cbiAgICB0cmVlLmNvbW1pdFVwZGF0ZSh1cGRhdGUpO1xuICB9KTtcbn1cbiJdfQ==