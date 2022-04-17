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
        define("@angular/language-service/src/reflector_host", ["require", "exports", "@angular/compiler-cli/src/language_services", "path", "typescript"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var language_services_1 = require("@angular/compiler-cli/src/language_services");
    var path = require("path");
    var ts = require("typescript");
    var ReflectorModuleModuleResolutionHost = /** @class */ (function () {
        function ReflectorModuleModuleResolutionHost(host, getProgram) {
            var _this = this;
            this.host = host;
            this.getProgram = getProgram;
            // Note: verboseInvalidExpressions is important so that
            // the collector will collect errors instead of throwing
            this.metadataCollector = new language_services_1.MetadataCollector({ verboseInvalidExpression: true });
            if (host.directoryExists)
                this.directoryExists = function (directoryName) { return _this.host.directoryExists(directoryName); };
        }
        ReflectorModuleModuleResolutionHost.prototype.fileExists = function (fileName) { return !!this.host.getScriptSnapshot(fileName); };
        ReflectorModuleModuleResolutionHost.prototype.readFile = function (fileName) {
            var snapshot = this.host.getScriptSnapshot(fileName);
            if (snapshot) {
                return snapshot.getText(0, snapshot.getLength());
            }
            // Typescript readFile() declaration should be `readFile(fileName: string): string | undefined
            return undefined;
        };
        ReflectorModuleModuleResolutionHost.prototype.getSourceFileMetadata = function (fileName) {
            var sf = this.getProgram().getSourceFile(fileName);
            return sf ? this.metadataCollector.getMetadata(sf) : undefined;
        };
        ReflectorModuleModuleResolutionHost.prototype.cacheMetadata = function (fileName) {
            // Don't cache the metadata for .ts files as they might change in the editor!
            return fileName.endsWith('.d.ts');
        };
        return ReflectorModuleModuleResolutionHost;
    }());
    var ReflectorHost = /** @class */ (function () {
        function ReflectorHost(getProgram, tsLSHost, _) {
            this.tsLSHost = tsLSHost;
            this.metadataReaderCache = language_services_1.createMetadataReaderCache();
            // tsLSHost.getCurrentDirectory() returns the directory where tsconfig.json
            // is located. This is not the same as process.cwd() because the language
            // service host sets the "project root path" as its current directory.
            var currentDir = tsLSHost.getCurrentDirectory();
            this.fakeContainingPath = currentDir ? path.join(currentDir, 'fakeContainingFile.ts') : '';
            this.hostAdapter = new ReflectorModuleModuleResolutionHost(tsLSHost, getProgram);
            this.moduleResolutionCache = ts.createModuleResolutionCache(currentDir, function (s) { return s; }, // getCanonicalFileName
            tsLSHost.getCompilationSettings());
        }
        ReflectorHost.prototype.getMetadataFor = function (modulePath) {
            return language_services_1.readMetadata(modulePath, this.hostAdapter, this.metadataReaderCache);
        };
        ReflectorHost.prototype.moduleNameToFileName = function (moduleName, containingFile) {
            if (!containingFile) {
                if (moduleName.startsWith('.')) {
                    throw new Error('Resolution of relative paths requires a containing file.');
                }
                if (!this.fakeContainingPath) {
                    // If current directory is empty then the file must belong to an inferred
                    // project (no tsconfig.json), in which case it's not possible to resolve
                    // the module without the caller explicitly providing a containing file.
                    throw new Error("Could not resolve '" + moduleName + "' without a containing file.");
                }
                containingFile = this.fakeContainingPath;
            }
            var compilerOptions = this.tsLSHost.getCompilationSettings();
            var resolved = ts.resolveModuleName(moduleName, containingFile, compilerOptions, this.hostAdapter, this.moduleResolutionCache)
                .resolvedModule;
            return resolved ? resolved.resolvedFileName : null;
        };
        ReflectorHost.prototype.getOutputName = function (filePath) { return filePath; };
        return ReflectorHost;
    }());
    exports.ReflectorHost = ReflectorHost;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVmbGVjdG9yX2hvc3QuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9sYW5ndWFnZS1zZXJ2aWNlL3NyYy9yZWZsZWN0b3JfaG9zdC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUdILGlGQUEySTtJQUMzSSwyQkFBNkI7SUFDN0IsK0JBQWlDO0lBRWpDO1FBS0UsNkNBQW9CLElBQTRCLEVBQVUsVUFBNEI7WUFBdEYsaUJBR0M7WUFIbUIsU0FBSSxHQUFKLElBQUksQ0FBd0I7WUFBVSxlQUFVLEdBQVYsVUFBVSxDQUFrQjtZQUp0Rix1REFBdUQ7WUFDdkQsd0RBQXdEO1lBQ2hELHNCQUFpQixHQUFHLElBQUkscUNBQWlCLENBQUMsRUFBQyx3QkFBd0IsRUFBRSxJQUFJLEVBQUMsQ0FBQyxDQUFDO1lBR2xGLElBQUksSUFBSSxDQUFDLGVBQWU7Z0JBQ3RCLElBQUksQ0FBQyxlQUFlLEdBQUcsVUFBQSxhQUFhLElBQUksT0FBQSxLQUFJLENBQUMsSUFBSSxDQUFDLGVBQWlCLENBQUMsYUFBYSxDQUFDLEVBQTFDLENBQTBDLENBQUM7UUFDdkYsQ0FBQztRQUVELHdEQUFVLEdBQVYsVUFBVyxRQUFnQixJQUFhLE9BQU8sQ0FBQyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsaUJBQWlCLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBRXpGLHNEQUFRLEdBQVIsVUFBUyxRQUFnQjtZQUN2QixJQUFJLFFBQVEsR0FBRyxJQUFJLENBQUMsSUFBSSxDQUFDLGlCQUFpQixDQUFDLFFBQVEsQ0FBQyxDQUFDO1lBQ3JELElBQUksUUFBUSxFQUFFO2dCQUNaLE9BQU8sUUFBUSxDQUFDLE9BQU8sQ0FBQyxDQUFDLEVBQUUsUUFBUSxDQUFDLFNBQVMsRUFBRSxDQUFDLENBQUM7YUFDbEQ7WUFFRCw4RkFBOEY7WUFDOUYsT0FBTyxTQUFXLENBQUM7UUFDckIsQ0FBQztRQUtELG1FQUFxQixHQUFyQixVQUFzQixRQUFnQjtZQUNwQyxJQUFNLEVBQUUsR0FBRyxJQUFJLENBQUMsVUFBVSxFQUFFLENBQUMsYUFBYSxDQUFDLFFBQVEsQ0FBQyxDQUFDO1lBQ3JELE9BQU8sRUFBRSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsaUJBQWlCLENBQUMsV0FBVyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxTQUFTLENBQUM7UUFDakUsQ0FBQztRQUVELDJEQUFhLEdBQWIsVUFBYyxRQUFnQjtZQUM1Qiw2RUFBNkU7WUFDN0UsT0FBTyxRQUFRLENBQUMsUUFBUSxDQUFDLE9BQU8sQ0FBQyxDQUFDO1FBQ3BDLENBQUM7UUFDSCwwQ0FBQztJQUFELENBQUMsQUFsQ0QsSUFrQ0M7SUFFRDtRQU1FLHVCQUNJLFVBQTRCLEVBQW1CLFFBQWdDLEVBQUUsQ0FBSztZQUF2QyxhQUFRLEdBQVIsUUFBUSxDQUF3QjtZQUxsRSx3QkFBbUIsR0FBRyw2Q0FBeUIsRUFBRSxDQUFDO1lBTWpFLDJFQUEyRTtZQUMzRSx5RUFBeUU7WUFDekUsc0VBQXNFO1lBQ3RFLElBQU0sVUFBVSxHQUFHLFFBQVEsQ0FBQyxtQkFBbUIsRUFBRSxDQUFDO1lBQ2xELElBQUksQ0FBQyxrQkFBa0IsR0FBRyxVQUFVLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsVUFBVSxFQUFFLHVCQUF1QixDQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQztZQUMzRixJQUFJLENBQUMsV0FBVyxHQUFHLElBQUksbUNBQW1DLENBQUMsUUFBUSxFQUFFLFVBQVUsQ0FBQyxDQUFDO1lBQ2pGLElBQUksQ0FBQyxxQkFBcUIsR0FBRyxFQUFFLENBQUMsMkJBQTJCLENBQ3ZELFVBQVUsRUFDVixVQUFBLENBQUMsSUFBSSxPQUFBLENBQUMsRUFBRCxDQUFDLEVBQUcsdUJBQXVCO1lBQ2hDLFFBQVEsQ0FBQyxzQkFBc0IsRUFBRSxDQUFDLENBQUM7UUFDekMsQ0FBQztRQUVELHNDQUFjLEdBQWQsVUFBZSxVQUFrQjtZQUMvQixPQUFPLGdDQUFZLENBQUMsVUFBVSxFQUFFLElBQUksQ0FBQyxXQUFXLEVBQUUsSUFBSSxDQUFDLG1CQUFtQixDQUFDLENBQUM7UUFDOUUsQ0FBQztRQUVELDRDQUFvQixHQUFwQixVQUFxQixVQUFrQixFQUFFLGNBQXVCO1lBQzlELElBQUksQ0FBQyxjQUFjLEVBQUU7Z0JBQ25CLElBQUksVUFBVSxDQUFDLFVBQVUsQ0FBQyxHQUFHLENBQUMsRUFBRTtvQkFDOUIsTUFBTSxJQUFJLEtBQUssQ0FBQywwREFBMEQsQ0FBQyxDQUFDO2lCQUM3RTtnQkFDRCxJQUFJLENBQUMsSUFBSSxDQUFDLGtCQUFrQixFQUFFO29CQUM1Qix5RUFBeUU7b0JBQ3pFLHlFQUF5RTtvQkFDekUsd0VBQXdFO29CQUN4RSxNQUFNLElBQUksS0FBSyxDQUFDLHdCQUFzQixVQUFVLGlDQUE4QixDQUFDLENBQUM7aUJBQ2pGO2dCQUNELGNBQWMsR0FBRyxJQUFJLENBQUMsa0JBQWtCLENBQUM7YUFDMUM7WUFDRCxJQUFNLGVBQWUsR0FBRyxJQUFJLENBQUMsUUFBUSxDQUFDLHNCQUFzQixFQUFFLENBQUM7WUFDL0QsSUFBTSxRQUFRLEdBQUcsRUFBRSxDQUFDLGlCQUFpQixDQUNkLFVBQVUsRUFBRSxjQUFjLEVBQUUsZUFBZSxFQUFFLElBQUksQ0FBQyxXQUFXLEVBQzdELElBQUksQ0FBQyxxQkFBcUIsQ0FBQztpQkFDNUIsY0FBYyxDQUFDO1lBQ3JDLE9BQU8sUUFBUSxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsZ0JBQWdCLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQztRQUNyRCxDQUFDO1FBRUQscUNBQWEsR0FBYixVQUFjLFFBQWdCLElBQUksT0FBTyxRQUFRLENBQUMsQ0FBQyxDQUFDO1FBQ3RELG9CQUFDO0lBQUQsQ0FBQyxBQTlDRCxJQThDQztJQTlDWSxzQ0FBYSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtTdGF0aWNTeW1ib2xSZXNvbHZlckhvc3R9IGZyb20gJ0Bhbmd1bGFyL2NvbXBpbGVyJztcbmltcG9ydCB7TWV0YWRhdGFDb2xsZWN0b3IsIE1ldGFkYXRhUmVhZGVySG9zdCwgY3JlYXRlTWV0YWRhdGFSZWFkZXJDYWNoZSwgcmVhZE1ldGFkYXRhfSBmcm9tICdAYW5ndWxhci9jb21waWxlci1jbGkvc3JjL2xhbmd1YWdlX3NlcnZpY2VzJztcbmltcG9ydCAqIGFzIHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgKiBhcyB0cyBmcm9tICd0eXBlc2NyaXB0JztcblxuY2xhc3MgUmVmbGVjdG9yTW9kdWxlTW9kdWxlUmVzb2x1dGlvbkhvc3QgaW1wbGVtZW50cyB0cy5Nb2R1bGVSZXNvbHV0aW9uSG9zdCwgTWV0YWRhdGFSZWFkZXJIb3N0IHtcbiAgLy8gTm90ZTogdmVyYm9zZUludmFsaWRFeHByZXNzaW9ucyBpcyBpbXBvcnRhbnQgc28gdGhhdFxuICAvLyB0aGUgY29sbGVjdG9yIHdpbGwgY29sbGVjdCBlcnJvcnMgaW5zdGVhZCBvZiB0aHJvd2luZ1xuICBwcml2YXRlIG1ldGFkYXRhQ29sbGVjdG9yID0gbmV3IE1ldGFkYXRhQ29sbGVjdG9yKHt2ZXJib3NlSW52YWxpZEV4cHJlc3Npb246IHRydWV9KTtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGhvc3Q6IHRzLkxhbmd1YWdlU2VydmljZUhvc3QsIHByaXZhdGUgZ2V0UHJvZ3JhbTogKCkgPT4gdHMuUHJvZ3JhbSkge1xuICAgIGlmIChob3N0LmRpcmVjdG9yeUV4aXN0cylcbiAgICAgIHRoaXMuZGlyZWN0b3J5RXhpc3RzID0gZGlyZWN0b3J5TmFtZSA9PiB0aGlzLmhvc3QuZGlyZWN0b3J5RXhpc3RzICEoZGlyZWN0b3J5TmFtZSk7XG4gIH1cblxuICBmaWxlRXhpc3RzKGZpbGVOYW1lOiBzdHJpbmcpOiBib29sZWFuIHsgcmV0dXJuICEhdGhpcy5ob3N0LmdldFNjcmlwdFNuYXBzaG90KGZpbGVOYW1lKTsgfVxuXG4gIHJlYWRGaWxlKGZpbGVOYW1lOiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIGxldCBzbmFwc2hvdCA9IHRoaXMuaG9zdC5nZXRTY3JpcHRTbmFwc2hvdChmaWxlTmFtZSk7XG4gICAgaWYgKHNuYXBzaG90KSB7XG4gICAgICByZXR1cm4gc25hcHNob3QuZ2V0VGV4dCgwLCBzbmFwc2hvdC5nZXRMZW5ndGgoKSk7XG4gICAgfVxuXG4gICAgLy8gVHlwZXNjcmlwdCByZWFkRmlsZSgpIGRlY2xhcmF0aW9uIHNob3VsZCBiZSBgcmVhZEZpbGUoZmlsZU5hbWU6IHN0cmluZyk6IHN0cmluZyB8IHVuZGVmaW5lZFxuICAgIHJldHVybiB1bmRlZmluZWQgITtcbiAgfVxuXG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBkaXJlY3RvcnlFeGlzdHMgITogKGRpcmVjdG9yeU5hbWU6IHN0cmluZykgPT4gYm9vbGVhbjtcblxuICBnZXRTb3VyY2VGaWxlTWV0YWRhdGEoZmlsZU5hbWU6IHN0cmluZykge1xuICAgIGNvbnN0IHNmID0gdGhpcy5nZXRQcm9ncmFtKCkuZ2V0U291cmNlRmlsZShmaWxlTmFtZSk7XG4gICAgcmV0dXJuIHNmID8gdGhpcy5tZXRhZGF0YUNvbGxlY3Rvci5nZXRNZXRhZGF0YShzZikgOiB1bmRlZmluZWQ7XG4gIH1cblxuICBjYWNoZU1ldGFkYXRhKGZpbGVOYW1lOiBzdHJpbmcpIHtcbiAgICAvLyBEb24ndCBjYWNoZSB0aGUgbWV0YWRhdGEgZm9yIC50cyBmaWxlcyBhcyB0aGV5IG1pZ2h0IGNoYW5nZSBpbiB0aGUgZWRpdG9yIVxuICAgIHJldHVybiBmaWxlTmFtZS5lbmRzV2l0aCgnLmQudHMnKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgUmVmbGVjdG9ySG9zdCBpbXBsZW1lbnRzIFN0YXRpY1N5bWJvbFJlc29sdmVySG9zdCB7XG4gIHByaXZhdGUgcmVhZG9ubHkgaG9zdEFkYXB0ZXI6IFJlZmxlY3Rvck1vZHVsZU1vZHVsZVJlc29sdXRpb25Ib3N0O1xuICBwcml2YXRlIHJlYWRvbmx5IG1ldGFkYXRhUmVhZGVyQ2FjaGUgPSBjcmVhdGVNZXRhZGF0YVJlYWRlckNhY2hlKCk7XG4gIHByaXZhdGUgcmVhZG9ubHkgbW9kdWxlUmVzb2x1dGlvbkNhY2hlOiB0cy5Nb2R1bGVSZXNvbHV0aW9uQ2FjaGU7XG4gIHByaXZhdGUgcmVhZG9ubHkgZmFrZUNvbnRhaW5pbmdQYXRoOiBzdHJpbmc7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBnZXRQcm9ncmFtOiAoKSA9PiB0cy5Qcm9ncmFtLCBwcml2YXRlIHJlYWRvbmx5IHRzTFNIb3N0OiB0cy5MYW5ndWFnZVNlcnZpY2VIb3N0LCBfOiB7fSkge1xuICAgIC8vIHRzTFNIb3N0LmdldEN1cnJlbnREaXJlY3RvcnkoKSByZXR1cm5zIHRoZSBkaXJlY3Rvcnkgd2hlcmUgdHNjb25maWcuanNvblxuICAgIC8vIGlzIGxvY2F0ZWQuIFRoaXMgaXMgbm90IHRoZSBzYW1lIGFzIHByb2Nlc3MuY3dkKCkgYmVjYXVzZSB0aGUgbGFuZ3VhZ2VcbiAgICAvLyBzZXJ2aWNlIGhvc3Qgc2V0cyB0aGUgXCJwcm9qZWN0IHJvb3QgcGF0aFwiIGFzIGl0cyBjdXJyZW50IGRpcmVjdG9yeS5cbiAgICBjb25zdCBjdXJyZW50RGlyID0gdHNMU0hvc3QuZ2V0Q3VycmVudERpcmVjdG9yeSgpO1xuICAgIHRoaXMuZmFrZUNvbnRhaW5pbmdQYXRoID0gY3VycmVudERpciA/IHBhdGguam9pbihjdXJyZW50RGlyLCAnZmFrZUNvbnRhaW5pbmdGaWxlLnRzJykgOiAnJztcbiAgICB0aGlzLmhvc3RBZGFwdGVyID0gbmV3IFJlZmxlY3Rvck1vZHVsZU1vZHVsZVJlc29sdXRpb25Ib3N0KHRzTFNIb3N0LCBnZXRQcm9ncmFtKTtcbiAgICB0aGlzLm1vZHVsZVJlc29sdXRpb25DYWNoZSA9IHRzLmNyZWF0ZU1vZHVsZVJlc29sdXRpb25DYWNoZShcbiAgICAgICAgY3VycmVudERpcixcbiAgICAgICAgcyA9PiBzLCAgLy8gZ2V0Q2Fub25pY2FsRmlsZU5hbWVcbiAgICAgICAgdHNMU0hvc3QuZ2V0Q29tcGlsYXRpb25TZXR0aW5ncygpKTtcbiAgfVxuXG4gIGdldE1ldGFkYXRhRm9yKG1vZHVsZVBhdGg6IHN0cmluZyk6IHtba2V5OiBzdHJpbmddOiBhbnl9W118dW5kZWZpbmVkIHtcbiAgICByZXR1cm4gcmVhZE1ldGFkYXRhKG1vZHVsZVBhdGgsIHRoaXMuaG9zdEFkYXB0ZXIsIHRoaXMubWV0YWRhdGFSZWFkZXJDYWNoZSk7XG4gIH1cblxuICBtb2R1bGVOYW1lVG9GaWxlTmFtZShtb2R1bGVOYW1lOiBzdHJpbmcsIGNvbnRhaW5pbmdGaWxlPzogc3RyaW5nKTogc3RyaW5nfG51bGwge1xuICAgIGlmICghY29udGFpbmluZ0ZpbGUpIHtcbiAgICAgIGlmIChtb2R1bGVOYW1lLnN0YXJ0c1dpdGgoJy4nKSkge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ1Jlc29sdXRpb24gb2YgcmVsYXRpdmUgcGF0aHMgcmVxdWlyZXMgYSBjb250YWluaW5nIGZpbGUuJyk7XG4gICAgICB9XG4gICAgICBpZiAoIXRoaXMuZmFrZUNvbnRhaW5pbmdQYXRoKSB7XG4gICAgICAgIC8vIElmIGN1cnJlbnQgZGlyZWN0b3J5IGlzIGVtcHR5IHRoZW4gdGhlIGZpbGUgbXVzdCBiZWxvbmcgdG8gYW4gaW5mZXJyZWRcbiAgICAgICAgLy8gcHJvamVjdCAobm8gdHNjb25maWcuanNvbiksIGluIHdoaWNoIGNhc2UgaXQncyBub3QgcG9zc2libGUgdG8gcmVzb2x2ZVxuICAgICAgICAvLyB0aGUgbW9kdWxlIHdpdGhvdXQgdGhlIGNhbGxlciBleHBsaWNpdGx5IHByb3ZpZGluZyBhIGNvbnRhaW5pbmcgZmlsZS5cbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBDb3VsZCBub3QgcmVzb2x2ZSAnJHttb2R1bGVOYW1lfScgd2l0aG91dCBhIGNvbnRhaW5pbmcgZmlsZS5gKTtcbiAgICAgIH1cbiAgICAgIGNvbnRhaW5pbmdGaWxlID0gdGhpcy5mYWtlQ29udGFpbmluZ1BhdGg7XG4gICAgfVxuICAgIGNvbnN0IGNvbXBpbGVyT3B0aW9ucyA9IHRoaXMudHNMU0hvc3QuZ2V0Q29tcGlsYXRpb25TZXR0aW5ncygpO1xuICAgIGNvbnN0IHJlc29sdmVkID0gdHMucmVzb2x2ZU1vZHVsZU5hbWUoXG4gICAgICAgICAgICAgICAgICAgICAgICAgICBtb2R1bGVOYW1lLCBjb250YWluaW5nRmlsZSwgY29tcGlsZXJPcHRpb25zLCB0aGlzLmhvc3RBZGFwdGVyLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgdGhpcy5tb2R1bGVSZXNvbHV0aW9uQ2FjaGUpXG4gICAgICAgICAgICAgICAgICAgICAgICAgLnJlc29sdmVkTW9kdWxlO1xuICAgIHJldHVybiByZXNvbHZlZCA/IHJlc29sdmVkLnJlc29sdmVkRmlsZU5hbWUgOiBudWxsO1xuICB9XG5cbiAgZ2V0T3V0cHV0TmFtZShmaWxlUGF0aDogc3RyaW5nKSB7IHJldHVybiBmaWxlUGF0aDsgfVxufVxuIl19