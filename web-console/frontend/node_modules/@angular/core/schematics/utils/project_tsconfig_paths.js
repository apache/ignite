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
        define("@angular/core/schematics/utils/project_tsconfig_paths", ["require", "exports", "@angular-devkit/core"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const core_1 = require("@angular-devkit/core");
    /** Name of the default Angular CLI workspace configuration files. */
    const defaultWorkspaceConfigPaths = ['/angular.json', '/.angular.json'];
    /**
     * Gets all tsconfig paths from a CLI project by reading the workspace configuration
     * and looking for common tsconfig locations.
     */
    function getProjectTsConfigPaths(tree) {
        // Start with some tsconfig paths that are generally used within CLI projects. Note
        // that we are not interested in IDE-specific tsconfig files (e.g. /tsconfig.json)
        const buildPaths = new Set(['src/tsconfig.app.json']);
        const testPaths = new Set(['src/tsconfig.spec.json']);
        // Add any tsconfig directly referenced in a build or test task of the angular.json workspace.
        const workspace = getWorkspaceConfigGracefully(tree);
        if (workspace) {
            const projects = Object.keys(workspace.projects).map(name => workspace.projects[name]);
            for (const project of projects) {
                const buildPath = getTargetTsconfigPath(project, 'build');
                const testPath = getTargetTsconfigPath(project, 'test');
                if (buildPath) {
                    buildPaths.add(buildPath);
                }
                if (testPath) {
                    testPaths.add(testPath);
                }
            }
        }
        // Filter out tsconfig files that don't exist in the CLI project.
        return {
            buildPaths: Array.from(buildPaths).filter(p => tree.exists(p)),
            testPaths: Array.from(testPaths).filter(p => tree.exists(p)),
        };
    }
    exports.getProjectTsConfigPaths = getProjectTsConfigPaths;
    /** Gets the tsconfig path from the given target within the specified project. */
    function getTargetTsconfigPath(project, targetName) {
        if (project.targets && project.targets[targetName] && project.targets[targetName].options &&
            project.targets[targetName].options.tsConfig) {
            return core_1.normalize(project.targets[targetName].options.tsConfig);
        }
        if (project.architect && project.architect[targetName] && project.architect[targetName].options &&
            project.architect[targetName].options.tsConfig) {
            return core_1.normalize(project.architect[targetName].options.tsConfig);
        }
        return null;
    }
    /**
     * Resolve the workspace configuration of the specified tree gracefully. We cannot use the utility
     * functions from the default Angular schematics because those might not be present in older
     * versions of the CLI. Also it's important to resolve the workspace gracefully because
     * the CLI project could be still using `.angular-cli.json` instead of thew new config.
     */
    function getWorkspaceConfigGracefully(tree) {
        const path = defaultWorkspaceConfigPaths.find(filePath => tree.exists(filePath));
        const configBuffer = tree.read(path);
        if (!path || !configBuffer) {
            return null;
        }
        try {
            // Parse the workspace file as JSON5 which is also supported for CLI
            // workspace configurations.
            return core_1.parseJson(configBuffer.toString(), core_1.JsonParseMode.Json5);
        }
        catch (e) {
            return null;
        }
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHJvamVjdF90c2NvbmZpZ19wYXRocy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc2NoZW1hdGljcy91dGlscy9wcm9qZWN0X3RzY29uZmlnX3BhdGhzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgsK0NBQXlFO0lBSXpFLHFFQUFxRTtJQUNyRSxNQUFNLDJCQUEyQixHQUFHLENBQUMsZUFBZSxFQUFFLGdCQUFnQixDQUFDLENBQUM7SUFFeEU7OztPQUdHO0lBQ0gsU0FBZ0IsdUJBQXVCLENBQUMsSUFBVTtRQUNoRCxtRkFBbUY7UUFDbkYsa0ZBQWtGO1FBQ2xGLE1BQU0sVUFBVSxHQUFHLElBQUksR0FBRyxDQUFTLENBQUMsdUJBQXVCLENBQUMsQ0FBQyxDQUFDO1FBQzlELE1BQU0sU0FBUyxHQUFHLElBQUksR0FBRyxDQUFTLENBQUMsd0JBQXdCLENBQUMsQ0FBQyxDQUFDO1FBRTlELDhGQUE4RjtRQUM5RixNQUFNLFNBQVMsR0FBRyw0QkFBNEIsQ0FBQyxJQUFJLENBQUMsQ0FBQztRQUVyRCxJQUFJLFNBQVMsRUFBRTtZQUNiLE1BQU0sUUFBUSxHQUFHLE1BQU0sQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLFFBQVEsQ0FBQyxDQUFDLEdBQUcsQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLFNBQVMsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQztZQUN2RixLQUFLLE1BQU0sT0FBTyxJQUFJLFFBQVEsRUFBRTtnQkFDOUIsTUFBTSxTQUFTLEdBQUcscUJBQXFCLENBQUMsT0FBTyxFQUFFLE9BQU8sQ0FBQyxDQUFDO2dCQUMxRCxNQUFNLFFBQVEsR0FBRyxxQkFBcUIsQ0FBQyxPQUFPLEVBQUUsTUFBTSxDQUFDLENBQUM7Z0JBRXhELElBQUksU0FBUyxFQUFFO29CQUNiLFVBQVUsQ0FBQyxHQUFHLENBQUMsU0FBUyxDQUFDLENBQUM7aUJBQzNCO2dCQUVELElBQUksUUFBUSxFQUFFO29CQUNaLFNBQVMsQ0FBQyxHQUFHLENBQUMsUUFBUSxDQUFDLENBQUM7aUJBQ3pCO2FBQ0Y7U0FDRjtRQUVELGlFQUFpRTtRQUNqRSxPQUFPO1lBQ0wsVUFBVSxFQUFFLEtBQUssQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsQ0FBQztZQUM5RCxTQUFTLEVBQUUsS0FBSyxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDO1NBQzdELENBQUM7SUFDSixDQUFDO0lBOUJELDBEQThCQztJQUVELGlGQUFpRjtJQUNqRixTQUFTLHFCQUFxQixDQUFDLE9BQXlCLEVBQUUsVUFBa0I7UUFDMUUsSUFBSSxPQUFPLENBQUMsT0FBTyxJQUFJLE9BQU8sQ0FBQyxPQUFPLENBQUMsVUFBVSxDQUFDLElBQUksT0FBTyxDQUFDLE9BQU8sQ0FBQyxVQUFVLENBQUMsQ0FBQyxPQUFPO1lBQ3JGLE9BQU8sQ0FBQyxPQUFPLENBQUMsVUFBVSxDQUFDLENBQUMsT0FBTyxDQUFDLFFBQVEsRUFBRTtZQUNoRCxPQUFPLGdCQUFTLENBQUMsT0FBTyxDQUFDLE9BQU8sQ0FBQyxVQUFVLENBQUMsQ0FBQyxPQUFPLENBQUMsUUFBUSxDQUFDLENBQUM7U0FDaEU7UUFFRCxJQUFJLE9BQU8sQ0FBQyxTQUFTLElBQUksT0FBTyxDQUFDLFNBQVMsQ0FBQyxVQUFVLENBQUMsSUFBSSxPQUFPLENBQUMsU0FBUyxDQUFDLFVBQVUsQ0FBQyxDQUFDLE9BQU87WUFDM0YsT0FBTyxDQUFDLFNBQVMsQ0FBQyxVQUFVLENBQUMsQ0FBQyxPQUFPLENBQUMsUUFBUSxFQUFFO1lBQ2xELE9BQU8sZ0JBQVMsQ0FBQyxPQUFPLENBQUMsU0FBUyxDQUFDLFVBQVUsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxRQUFRLENBQUMsQ0FBQztTQUNsRTtRQUNELE9BQU8sSUFBSSxDQUFDO0lBQ2QsQ0FBQztJQUVEOzs7OztPQUtHO0lBQ0gsU0FBUyw0QkFBNEIsQ0FBQyxJQUFVO1FBQzlDLE1BQU0sSUFBSSxHQUFHLDJCQUEyQixDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsRUFBRSxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQztRQUNqRixNQUFNLFlBQVksR0FBRyxJQUFJLENBQUMsSUFBSSxDQUFDLElBQU0sQ0FBQyxDQUFDO1FBRXZDLElBQUksQ0FBQyxJQUFJLElBQUksQ0FBQyxZQUFZLEVBQUU7WUFDMUIsT0FBTyxJQUFJLENBQUM7U0FDYjtRQUVELElBQUk7WUFDRixvRUFBb0U7WUFDcEUsNEJBQTRCO1lBQzVCLE9BQU8sZ0JBQVMsQ0FBQyxZQUFZLENBQUMsUUFBUSxFQUFFLEVBQUUsb0JBQWEsQ0FBQyxLQUFLLENBQUMsQ0FBQztTQUNoRTtRQUFDLE9BQU8sQ0FBQyxFQUFFO1lBQ1YsT0FBTyxJQUFJLENBQUM7U0FDYjtJQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SnNvblBhcnNlTW9kZSwgbm9ybWFsaXplLCBwYXJzZUpzb259IGZyb20gJ0Bhbmd1bGFyLWRldmtpdC9jb3JlJztcbmltcG9ydCB7VHJlZX0gZnJvbSAnQGFuZ3VsYXItZGV2a2l0L3NjaGVtYXRpY3MnO1xuaW1wb3J0IHtXb3Jrc3BhY2VQcm9qZWN0fSBmcm9tICdAc2NoZW1hdGljcy9hbmd1bGFyL3V0aWxpdHkvd29ya3NwYWNlLW1vZGVscyc7XG5cbi8qKiBOYW1lIG9mIHRoZSBkZWZhdWx0IEFuZ3VsYXIgQ0xJIHdvcmtzcGFjZSBjb25maWd1cmF0aW9uIGZpbGVzLiAqL1xuY29uc3QgZGVmYXVsdFdvcmtzcGFjZUNvbmZpZ1BhdGhzID0gWycvYW5ndWxhci5qc29uJywgJy8uYW5ndWxhci5qc29uJ107XG5cbi8qKlxuICogR2V0cyBhbGwgdHNjb25maWcgcGF0aHMgZnJvbSBhIENMSSBwcm9qZWN0IGJ5IHJlYWRpbmcgdGhlIHdvcmtzcGFjZSBjb25maWd1cmF0aW9uXG4gKiBhbmQgbG9va2luZyBmb3IgY29tbW9uIHRzY29uZmlnIGxvY2F0aW9ucy5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldFByb2plY3RUc0NvbmZpZ1BhdGhzKHRyZWU6IFRyZWUpOiB7YnVpbGRQYXRoczogc3RyaW5nW10sIHRlc3RQYXRoczogc3RyaW5nW119IHtcbiAgLy8gU3RhcnQgd2l0aCBzb21lIHRzY29uZmlnIHBhdGhzIHRoYXQgYXJlIGdlbmVyYWxseSB1c2VkIHdpdGhpbiBDTEkgcHJvamVjdHMuIE5vdGVcbiAgLy8gdGhhdCB3ZSBhcmUgbm90IGludGVyZXN0ZWQgaW4gSURFLXNwZWNpZmljIHRzY29uZmlnIGZpbGVzIChlLmcuIC90c2NvbmZpZy5qc29uKVxuICBjb25zdCBidWlsZFBhdGhzID0gbmV3IFNldDxzdHJpbmc+KFsnc3JjL3RzY29uZmlnLmFwcC5qc29uJ10pO1xuICBjb25zdCB0ZXN0UGF0aHMgPSBuZXcgU2V0PHN0cmluZz4oWydzcmMvdHNjb25maWcuc3BlYy5qc29uJ10pO1xuXG4gIC8vIEFkZCBhbnkgdHNjb25maWcgZGlyZWN0bHkgcmVmZXJlbmNlZCBpbiBhIGJ1aWxkIG9yIHRlc3QgdGFzayBvZiB0aGUgYW5ndWxhci5qc29uIHdvcmtzcGFjZS5cbiAgY29uc3Qgd29ya3NwYWNlID0gZ2V0V29ya3NwYWNlQ29uZmlnR3JhY2VmdWxseSh0cmVlKTtcblxuICBpZiAod29ya3NwYWNlKSB7XG4gICAgY29uc3QgcHJvamVjdHMgPSBPYmplY3Qua2V5cyh3b3Jrc3BhY2UucHJvamVjdHMpLm1hcChuYW1lID0+IHdvcmtzcGFjZS5wcm9qZWN0c1tuYW1lXSk7XG4gICAgZm9yIChjb25zdCBwcm9qZWN0IG9mIHByb2plY3RzKSB7XG4gICAgICBjb25zdCBidWlsZFBhdGggPSBnZXRUYXJnZXRUc2NvbmZpZ1BhdGgocHJvamVjdCwgJ2J1aWxkJyk7XG4gICAgICBjb25zdCB0ZXN0UGF0aCA9IGdldFRhcmdldFRzY29uZmlnUGF0aChwcm9qZWN0LCAndGVzdCcpO1xuXG4gICAgICBpZiAoYnVpbGRQYXRoKSB7XG4gICAgICAgIGJ1aWxkUGF0aHMuYWRkKGJ1aWxkUGF0aCk7XG4gICAgICB9XG5cbiAgICAgIGlmICh0ZXN0UGF0aCkge1xuICAgICAgICB0ZXN0UGF0aHMuYWRkKHRlc3RQYXRoKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICAvLyBGaWx0ZXIgb3V0IHRzY29uZmlnIGZpbGVzIHRoYXQgZG9uJ3QgZXhpc3QgaW4gdGhlIENMSSBwcm9qZWN0LlxuICByZXR1cm4ge1xuICAgIGJ1aWxkUGF0aHM6IEFycmF5LmZyb20oYnVpbGRQYXRocykuZmlsdGVyKHAgPT4gdHJlZS5leGlzdHMocCkpLFxuICAgIHRlc3RQYXRoczogQXJyYXkuZnJvbSh0ZXN0UGF0aHMpLmZpbHRlcihwID0+IHRyZWUuZXhpc3RzKHApKSxcbiAgfTtcbn1cblxuLyoqIEdldHMgdGhlIHRzY29uZmlnIHBhdGggZnJvbSB0aGUgZ2l2ZW4gdGFyZ2V0IHdpdGhpbiB0aGUgc3BlY2lmaWVkIHByb2plY3QuICovXG5mdW5jdGlvbiBnZXRUYXJnZXRUc2NvbmZpZ1BhdGgocHJvamVjdDogV29ya3NwYWNlUHJvamVjdCwgdGFyZ2V0TmFtZTogc3RyaW5nKTogc3RyaW5nfG51bGwge1xuICBpZiAocHJvamVjdC50YXJnZXRzICYmIHByb2plY3QudGFyZ2V0c1t0YXJnZXROYW1lXSAmJiBwcm9qZWN0LnRhcmdldHNbdGFyZ2V0TmFtZV0ub3B0aW9ucyAmJlxuICAgICAgcHJvamVjdC50YXJnZXRzW3RhcmdldE5hbWVdLm9wdGlvbnMudHNDb25maWcpIHtcbiAgICByZXR1cm4gbm9ybWFsaXplKHByb2plY3QudGFyZ2V0c1t0YXJnZXROYW1lXS5vcHRpb25zLnRzQ29uZmlnKTtcbiAgfVxuXG4gIGlmIChwcm9qZWN0LmFyY2hpdGVjdCAmJiBwcm9qZWN0LmFyY2hpdGVjdFt0YXJnZXROYW1lXSAmJiBwcm9qZWN0LmFyY2hpdGVjdFt0YXJnZXROYW1lXS5vcHRpb25zICYmXG4gICAgICBwcm9qZWN0LmFyY2hpdGVjdFt0YXJnZXROYW1lXS5vcHRpb25zLnRzQ29uZmlnKSB7XG4gICAgcmV0dXJuIG5vcm1hbGl6ZShwcm9qZWN0LmFyY2hpdGVjdFt0YXJnZXROYW1lXS5vcHRpb25zLnRzQ29uZmlnKTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cblxuLyoqXG4gKiBSZXNvbHZlIHRoZSB3b3Jrc3BhY2UgY29uZmlndXJhdGlvbiBvZiB0aGUgc3BlY2lmaWVkIHRyZWUgZ3JhY2VmdWxseS4gV2UgY2Fubm90IHVzZSB0aGUgdXRpbGl0eVxuICogZnVuY3Rpb25zIGZyb20gdGhlIGRlZmF1bHQgQW5ndWxhciBzY2hlbWF0aWNzIGJlY2F1c2UgdGhvc2UgbWlnaHQgbm90IGJlIHByZXNlbnQgaW4gb2xkZXJcbiAqIHZlcnNpb25zIG9mIHRoZSBDTEkuIEFsc28gaXQncyBpbXBvcnRhbnQgdG8gcmVzb2x2ZSB0aGUgd29ya3NwYWNlIGdyYWNlZnVsbHkgYmVjYXVzZVxuICogdGhlIENMSSBwcm9qZWN0IGNvdWxkIGJlIHN0aWxsIHVzaW5nIGAuYW5ndWxhci1jbGkuanNvbmAgaW5zdGVhZCBvZiB0aGV3IG5ldyBjb25maWcuXG4gKi9cbmZ1bmN0aW9uIGdldFdvcmtzcGFjZUNvbmZpZ0dyYWNlZnVsbHkodHJlZTogVHJlZSk6IGFueSB7XG4gIGNvbnN0IHBhdGggPSBkZWZhdWx0V29ya3NwYWNlQ29uZmlnUGF0aHMuZmluZChmaWxlUGF0aCA9PiB0cmVlLmV4aXN0cyhmaWxlUGF0aCkpO1xuICBjb25zdCBjb25maWdCdWZmZXIgPSB0cmVlLnJlYWQocGF0aCAhKTtcblxuICBpZiAoIXBhdGggfHwgIWNvbmZpZ0J1ZmZlcikge1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgdHJ5IHtcbiAgICAvLyBQYXJzZSB0aGUgd29ya3NwYWNlIGZpbGUgYXMgSlNPTjUgd2hpY2ggaXMgYWxzbyBzdXBwb3J0ZWQgZm9yIENMSVxuICAgIC8vIHdvcmtzcGFjZSBjb25maWd1cmF0aW9ucy5cbiAgICByZXR1cm4gcGFyc2VKc29uKGNvbmZpZ0J1ZmZlci50b1N0cmluZygpLCBKc29uUGFyc2VNb2RlLkpzb241KTtcbiAgfSBjYXRjaCAoZSkge1xuICAgIHJldHVybiBudWxsO1xuICB9XG59XG4iXX0=