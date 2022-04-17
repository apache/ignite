/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/core/schematics/migrations/static-queries", ["require", "exports", "@angular-devkit/schematics", "path", "rxjs", "typescript", "@angular/core/schematics/utils/ng_component_template", "@angular/core/schematics/utils/project_tsconfig_paths", "@angular/core/schematics/utils/typescript/parse_tsconfig", "@angular/core/schematics/migrations/static-queries/angular/ng_query_visitor", "@angular/core/schematics/migrations/static-queries/strategies/template_strategy/template_strategy", "@angular/core/schematics/migrations/static-queries/strategies/test_strategy/test_strategy", "@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/usage_strategy", "@angular/core/schematics/migrations/static-queries/transform"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const schematics_1 = require("@angular-devkit/schematics");
    const path_1 = require("path");
    const rxjs_1 = require("rxjs");
    const ts = require("typescript");
    const ng_component_template_1 = require("@angular/core/schematics/utils/ng_component_template");
    const project_tsconfig_paths_1 = require("@angular/core/schematics/utils/project_tsconfig_paths");
    const parse_tsconfig_1 = require("@angular/core/schematics/utils/typescript/parse_tsconfig");
    const ng_query_visitor_1 = require("@angular/core/schematics/migrations/static-queries/angular/ng_query_visitor");
    const template_strategy_1 = require("@angular/core/schematics/migrations/static-queries/strategies/template_strategy/template_strategy");
    const test_strategy_1 = require("@angular/core/schematics/migrations/static-queries/strategies/test_strategy/test_strategy");
    const usage_strategy_1 = require("@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/usage_strategy");
    const transform_1 = require("@angular/core/schematics/migrations/static-queries/transform");
    var SELECTED_STRATEGY;
    (function (SELECTED_STRATEGY) {
        SELECTED_STRATEGY[SELECTED_STRATEGY["TEMPLATE"] = 0] = "TEMPLATE";
        SELECTED_STRATEGY[SELECTED_STRATEGY["USAGE"] = 1] = "USAGE";
        SELECTED_STRATEGY[SELECTED_STRATEGY["TESTS"] = 2] = "TESTS";
    })(SELECTED_STRATEGY || (SELECTED_STRATEGY = {}));
    /** Entry point for the V8 static-query migration. */
    function default_1() {
        return (tree, context) => {
            // We need to cast the returned "Observable" to "any" as there is a
            // RxJS version mismatch that breaks the TS compilation.
            return rxjs_1.from(runMigration(tree, context).then(() => tree));
        };
    }
    exports.default = default_1;
    /** Runs the V8 migration static-query migration for all determined TypeScript projects. */
    function runMigration(tree, context) {
        return __awaiter(this, void 0, void 0, function* () {
            const { buildPaths, testPaths } = project_tsconfig_paths_1.getProjectTsConfigPaths(tree);
            const basePath = process.cwd();
            const logger = context.logger;
            logger.info('------ Static Query Migration ------');
            logger.info('With Angular version 8, developers need to');
            logger.info('explicitly specify the timing of ViewChild and');
            logger.info('ContentChild queries. Read more about this here:');
            logger.info('https://v8.angular.io/guide/static-query-migration');
            if (!buildPaths.length && !testPaths.length) {
                throw new schematics_1.SchematicsException('Could not find any tsconfig file. Cannot migrate queries ' +
                    'to add static flag.');
            }
            const analyzedFiles = new Set();
            const buildProjects = new Set();
            const failures = [];
            const strategy = process.env['NG_STATIC_QUERY_USAGE_STRATEGY'] === 'true' ?
                SELECTED_STRATEGY.USAGE :
                SELECTED_STRATEGY.TEMPLATE;
            for (const tsconfigPath of buildPaths) {
                const project = analyzeProject(tree, tsconfigPath, basePath, analyzedFiles, logger);
                if (project) {
                    buildProjects.add(project);
                }
            }
            if (buildProjects.size) {
                for (let project of Array.from(buildProjects.values())) {
                    failures.push(...yield runStaticQueryMigration(tree, project, strategy, logger));
                }
            }
            // For the "test" tsconfig projects we always want to use the test strategy as
            // we can't detect the proper timing within spec files.
            for (const tsconfigPath of testPaths) {
                const project = yield analyzeProject(tree, tsconfigPath, basePath, analyzedFiles, logger);
                if (project) {
                    failures.push(...yield runStaticQueryMigration(tree, project, SELECTED_STRATEGY.TESTS, logger));
                }
            }
            if (failures.length) {
                logger.info('');
                logger.info('Some queries could not be migrated automatically. Please go');
                logger.info('through these manually and apply the appropriate timing.');
                logger.info('For more info on how to choose a flag, please see: ');
                logger.info('https://v8.angular.io/guide/static-query-migration');
                failures.forEach(failure => logger.warn(`⮑   ${failure}`));
            }
            logger.info('------------------------------------------------');
        });
    }
    /**
     * Analyzes the given TypeScript project by looking for queries that need to be
     * migrated. In case there are no queries that can be migrated, null is returned.
     */
    function analyzeProject(tree, tsconfigPath, basePath, analyzedFiles, logger) {
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
        const syntacticDiagnostics = program.getSyntacticDiagnostics();
        // Syntactic TypeScript errors can throw off the query analysis and therefore we want
        // to notify the developer that we couldn't analyze parts of the project. Developers
        // can just re-run the migration after fixing these failures.
        if (syntacticDiagnostics.length) {
            logger.warn(`\nTypeScript project "${tsconfigPath}" has syntactical errors which could cause ` +
                `an incomplete migration. Please fix the following failures and rerun the migration:`);
            logger.error(ts.formatDiagnostics(syntacticDiagnostics, host));
            logger.info('Migration can be rerun with: "ng update @angular/core --from 7 --to 8 --migrate-only"\n');
        }
        const typeChecker = program.getTypeChecker();
        const sourceFiles = program.getSourceFiles().filter(f => !f.isDeclarationFile && !program.isSourceFileFromExternalLibrary(f));
        const queryVisitor = new ng_query_visitor_1.NgQueryResolveVisitor(typeChecker);
        // Analyze all project source-files and collect all queries that
        // need to be migrated.
        sourceFiles.forEach(sourceFile => {
            const relativePath = path_1.relative(basePath, sourceFile.fileName);
            // Only look for queries within the current source files if the
            // file has not been analyzed before.
            if (!analyzedFiles.has(relativePath)) {
                analyzedFiles.add(relativePath);
                queryVisitor.visitNode(sourceFile);
            }
        });
        if (queryVisitor.resolvedQueries.size === 0) {
            return null;
        }
        return { program, host, tsconfigPath, typeChecker, basePath, queryVisitor, sourceFiles };
    }
    /**
     * Runs the static query migration for the given project. The schematic analyzes all
     * queries within the project and sets up the query timing based on the current usage
     * of the query property. e.g. a view query that is not used in any lifecycle hook does
     * not need to be static and can be set up with "static: false".
     */
    function runStaticQueryMigration(tree, project, selectedStrategy, logger) {
        return __awaiter(this, void 0, void 0, function* () {
            const { sourceFiles, typeChecker, host, queryVisitor, tsconfigPath, basePath } = project;
            const printer = ts.createPrinter();
            const failureMessages = [];
            const templateVisitor = new ng_component_template_1.NgComponentTemplateVisitor(typeChecker);
            // If the "usage" strategy is selected, we also need to add the query visitor
            // to the analysis visitors so that query usage in templates can be also checked.
            if (selectedStrategy === SELECTED_STRATEGY.USAGE) {
                sourceFiles.forEach(s => templateVisitor.visitNode(s));
            }
            const { resolvedQueries, classMetadata } = queryVisitor;
            const { resolvedTemplates } = templateVisitor;
            if (selectedStrategy === SELECTED_STRATEGY.USAGE) {
                // Add all resolved templates to the class metadata if the usage strategy is used. This
                // is necessary in order to be able to check component templates for static query usage.
                resolvedTemplates.forEach(template => {
                    if (classMetadata.has(template.container)) {
                        classMetadata.get(template.container).template = template;
                    }
                });
            }
            let strategy;
            if (selectedStrategy === SELECTED_STRATEGY.USAGE) {
                strategy = new usage_strategy_1.QueryUsageStrategy(classMetadata, typeChecker);
            }
            else if (selectedStrategy === SELECTED_STRATEGY.TESTS) {
                strategy = new test_strategy_1.QueryTestStrategy();
            }
            else {
                strategy = new template_strategy_1.QueryTemplateStrategy(tsconfigPath, classMetadata, host);
            }
            try {
                strategy.setup();
            }
            catch (e) {
                if (selectedStrategy === SELECTED_STRATEGY.TEMPLATE) {
                    logger.warn(`\nThe template migration strategy uses the Angular compiler ` +
                        `internally and therefore projects that no longer build successfully after ` +
                        `the update cannot use the template migration strategy. Please ensure ` +
                        `there are no AOT compilation errors.\n`);
                }
                // In case the strategy could not be set up properly, we just exit the
                // migration. We don't want to throw an exception as this could mean
                // that other migrations are interrupted.
                logger.warn(`Could not setup migration strategy for "${project.tsconfigPath}". The ` +
                    `following error has been reported:\n`);
                logger.error(`${e.toString()}\n`);
                logger.info('Migration can be rerun with: "ng update @angular/core --from 7 --to 8 --migrate-only"\n');
                return [];
            }
            // Walk through all source files that contain resolved queries and update
            // the source files if needed. Note that we need to update multiple queries
            // within a source file within the same recorder in order to not throw off
            // the TypeScript node offsets.
            resolvedQueries.forEach((queries, sourceFile) => {
                const relativePath = path_1.relative(basePath, sourceFile.fileName);
                const update = tree.beginUpdate(relativePath);
                // Compute the query timing for all resolved queries and update the
                // query definitions to explicitly set the determined query timing.
                queries.forEach(q => {
                    const queryExpr = q.decorator.node.expression;
                    const { timing, message } = strategy.detectTiming(q);
                    const result = transform_1.getTransformedQueryCallExpr(q, timing, !!message);
                    if (!result) {
                        return;
                    }
                    const newText = printer.printNode(ts.EmitHint.Unspecified, result.node, sourceFile);
                    // Replace the existing query decorator call expression with the updated
                    // call expression node.
                    update.remove(queryExpr.getStart(), queryExpr.getWidth());
                    update.insertRight(queryExpr.getStart(), newText);
                    if (result.failureMessage || message) {
                        const { line, character } = ts.getLineAndCharacterOfPosition(sourceFile, q.decorator.node.getStart());
                        failureMessages.push(`${relativePath}@${line + 1}:${character + 1}: ${result.failureMessage || message}`);
                    }
                });
                tree.commitUpdate(update);
            });
            return failureMessages;
        });
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NjaGVtYXRpY3MvbWlncmF0aW9ucy9zdGF0aWMtcXVlcmllcy9pbmRleC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBR0gsMkRBQTZGO0lBQzdGLCtCQUF1QztJQUN2QywrQkFBMEI7SUFDMUIsaUNBQWlDO0lBRWpDLGdHQUE2RTtJQUM3RSxrR0FBMkU7SUFDM0UsNkZBQXdFO0lBRXhFLGtIQUFpRTtJQUNqRSx5SUFBdUY7SUFDdkYsNkhBQTJFO0lBRTNFLGdJQUE4RTtJQUM5RSw0RkFBd0Q7SUFFeEQsSUFBSyxpQkFJSjtJQUpELFdBQUssaUJBQWlCO1FBQ3BCLGlFQUFRLENBQUE7UUFDUiwyREFBSyxDQUFBO1FBQ0wsMkRBQUssQ0FBQTtJQUNQLENBQUMsRUFKSSxpQkFBaUIsS0FBakIsaUJBQWlCLFFBSXJCO0lBWUQscURBQXFEO0lBQ3JEO1FBQ0UsT0FBTyxDQUFDLElBQVUsRUFBRSxPQUF5QixFQUFFLEVBQUU7WUFDL0MsbUVBQW1FO1lBQ25FLHdEQUF3RDtZQUN4RCxPQUFPLFdBQUksQ0FBQyxZQUFZLENBQUMsSUFBSSxFQUFFLE9BQU8sQ0FBQyxDQUFDLElBQUksQ0FBQyxHQUFHLEVBQUUsQ0FBQyxJQUFJLENBQUMsQ0FBUSxDQUFDO1FBQ25FLENBQUMsQ0FBQztJQUNKLENBQUM7SUFORCw0QkFNQztJQUVELDJGQUEyRjtJQUMzRixTQUFlLFlBQVksQ0FBQyxJQUFVLEVBQUUsT0FBeUI7O1lBQy9ELE1BQU0sRUFBQyxVQUFVLEVBQUUsU0FBUyxFQUFDLEdBQUcsZ0RBQXVCLENBQUMsSUFBSSxDQUFDLENBQUM7WUFDOUQsTUFBTSxRQUFRLEdBQUcsT0FBTyxDQUFDLEdBQUcsRUFBRSxDQUFDO1lBQy9CLE1BQU0sTUFBTSxHQUFHLE9BQU8sQ0FBQyxNQUFNLENBQUM7WUFFOUIsTUFBTSxDQUFDLElBQUksQ0FBQyxzQ0FBc0MsQ0FBQyxDQUFDO1lBQ3BELE1BQU0sQ0FBQyxJQUFJLENBQUMsNENBQTRDLENBQUMsQ0FBQztZQUMxRCxNQUFNLENBQUMsSUFBSSxDQUFDLGdEQUFnRCxDQUFDLENBQUM7WUFDOUQsTUFBTSxDQUFDLElBQUksQ0FBQyxrREFBa0QsQ0FBQyxDQUFDO1lBQ2hFLE1BQU0sQ0FBQyxJQUFJLENBQUMsb0RBQW9ELENBQUMsQ0FBQztZQUVsRSxJQUFJLENBQUMsVUFBVSxDQUFDLE1BQU0sSUFBSSxDQUFDLFNBQVMsQ0FBQyxNQUFNLEVBQUU7Z0JBQzNDLE1BQU0sSUFBSSxnQ0FBbUIsQ0FDekIsMkRBQTJEO29CQUMzRCxxQkFBcUIsQ0FBQyxDQUFDO2FBQzVCO1lBRUQsTUFBTSxhQUFhLEdBQUcsSUFBSSxHQUFHLEVBQVUsQ0FBQztZQUN4QyxNQUFNLGFBQWEsR0FBRyxJQUFJLEdBQUcsRUFBbUIsQ0FBQztZQUNqRCxNQUFNLFFBQVEsR0FBRyxFQUFFLENBQUM7WUFDcEIsTUFBTSxRQUFRLEdBQUcsT0FBTyxDQUFDLEdBQUcsQ0FBQyxnQ0FBZ0MsQ0FBQyxLQUFLLE1BQU0sQ0FBQyxDQUFDO2dCQUN2RSxpQkFBaUIsQ0FBQyxLQUFLLENBQUMsQ0FBQztnQkFDekIsaUJBQWlCLENBQUMsUUFBUSxDQUFDO1lBRS9CLEtBQUssTUFBTSxZQUFZLElBQUksVUFBVSxFQUFFO2dCQUNyQyxNQUFNLE9BQU8sR0FBRyxjQUFjLENBQUMsSUFBSSxFQUFFLFlBQVksRUFBRSxRQUFRLEVBQUUsYUFBYSxFQUFFLE1BQU0sQ0FBQyxDQUFDO2dCQUNwRixJQUFJLE9BQU8sRUFBRTtvQkFDWCxhQUFhLENBQUMsR0FBRyxDQUFDLE9BQU8sQ0FBQyxDQUFDO2lCQUM1QjthQUNGO1lBRUQsSUFBSSxhQUFhLENBQUMsSUFBSSxFQUFFO2dCQUN0QixLQUFLLElBQUksT0FBTyxJQUFJLEtBQUssQ0FBQyxJQUFJLENBQUMsYUFBYSxDQUFDLE1BQU0sRUFBRSxDQUFDLEVBQUU7b0JBQ3RELFFBQVEsQ0FBQyxJQUFJLENBQUMsR0FBRyxNQUFNLHVCQUF1QixDQUFDLElBQUksRUFBRSxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sQ0FBQyxDQUFDLENBQUM7aUJBQ2xGO2FBQ0Y7WUFFRCw4RUFBOEU7WUFDOUUsdURBQXVEO1lBQ3ZELEtBQUssTUFBTSxZQUFZLElBQUksU0FBUyxFQUFFO2dCQUNwQyxNQUFNLE9BQU8sR0FBRyxNQUFNLGNBQWMsQ0FBQyxJQUFJLEVBQUUsWUFBWSxFQUFFLFFBQVEsRUFBRSxhQUFhLEVBQUUsTUFBTSxDQUFDLENBQUM7Z0JBQzFGLElBQUksT0FBTyxFQUFFO29CQUNYLFFBQVEsQ0FBQyxJQUFJLENBQ1QsR0FBRyxNQUFNLHVCQUF1QixDQUFDLElBQUksRUFBRSxPQUFPLEVBQUUsaUJBQWlCLENBQUMsS0FBSyxFQUFFLE1BQU0sQ0FBQyxDQUFDLENBQUM7aUJBQ3ZGO2FBQ0Y7WUFFRCxJQUFJLFFBQVEsQ0FBQyxNQUFNLEVBQUU7Z0JBQ25CLE1BQU0sQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUM7Z0JBQ2hCLE1BQU0sQ0FBQyxJQUFJLENBQUMsNkRBQTZELENBQUMsQ0FBQztnQkFDM0UsTUFBTSxDQUFDLElBQUksQ0FBQywwREFBMEQsQ0FBQyxDQUFDO2dCQUN4RSxNQUFNLENBQUMsSUFBSSxDQUFDLHFEQUFxRCxDQUFDLENBQUM7Z0JBQ25FLE1BQU0sQ0FBQyxJQUFJLENBQUMsb0RBQW9ELENBQUMsQ0FBQztnQkFDbEUsUUFBUSxDQUFDLE9BQU8sQ0FBQyxPQUFPLENBQUMsRUFBRSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsT0FBTyxPQUFPLEVBQUUsQ0FBQyxDQUFDLENBQUM7YUFDNUQ7WUFFRCxNQUFNLENBQUMsSUFBSSxDQUFDLGtEQUFrRCxDQUFDLENBQUM7UUFDbEUsQ0FBQztLQUFBO0lBRUQ7OztPQUdHO0lBQ0gsU0FBUyxjQUFjLENBQ25CLElBQVUsRUFBRSxZQUFvQixFQUFFLFFBQWdCLEVBQUUsYUFBMEIsRUFDOUUsTUFBeUI7UUFFdkIsTUFBTSxNQUFNLEdBQUcsa0NBQWlCLENBQUMsWUFBWSxFQUFFLGNBQU8sQ0FBQyxZQUFZLENBQUMsQ0FBQyxDQUFDO1FBQ3RFLE1BQU0sSUFBSSxHQUFHLEVBQUUsQ0FBQyxrQkFBa0IsQ0FBQyxNQUFNLENBQUMsT0FBTyxFQUFFLElBQUksQ0FBQyxDQUFDO1FBRXpELDZFQUE2RTtRQUM3RSwrRUFBK0U7UUFDL0UsOEVBQThFO1FBQzlFLHlFQUF5RTtRQUN6RSxJQUFJLENBQUMsUUFBUSxHQUFHLFFBQVEsQ0FBQyxFQUFFO1lBQ3pCLE1BQU0sTUFBTSxHQUFHLElBQUksQ0FBQyxJQUFJLENBQUMsZUFBUSxDQUFDLFFBQVEsRUFBRSxRQUFRLENBQUMsQ0FBQyxDQUFDO1lBQ3ZELGdGQUFnRjtZQUNoRix1Q0FBdUM7WUFDdkMscURBQXFEO1lBQ3JELE9BQU8sTUFBTSxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsUUFBUSxFQUFFLENBQUMsT0FBTyxDQUFDLFNBQVMsRUFBRSxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUMsU0FBUyxDQUFDO1FBQ3ZFLENBQUMsQ0FBQztRQUVGLE1BQU0sT0FBTyxHQUFHLEVBQUUsQ0FBQyxhQUFhLENBQUMsTUFBTSxDQUFDLFNBQVMsRUFBRSxNQUFNLENBQUMsT0FBTyxFQUFFLElBQUksQ0FBQyxDQUFDO1FBQ3pFLE1BQU0sb0JBQW9CLEdBQUcsT0FBTyxDQUFDLHVCQUF1QixFQUFFLENBQUM7UUFFL0QscUZBQXFGO1FBQ3JGLG9GQUFvRjtRQUNwRiw2REFBNkQ7UUFDN0QsSUFBSSxvQkFBb0IsQ0FBQyxNQUFNLEVBQUU7WUFDL0IsTUFBTSxDQUFDLElBQUksQ0FDUCx5QkFBeUIsWUFBWSw2Q0FBNkM7Z0JBQ2xGLHFGQUFxRixDQUFDLENBQUM7WUFDM0YsTUFBTSxDQUFDLEtBQUssQ0FBQyxFQUFFLENBQUMsaUJBQWlCLENBQUMsb0JBQW9CLEVBQUUsSUFBSSxDQUFDLENBQUMsQ0FBQztZQUMvRCxNQUFNLENBQUMsSUFBSSxDQUNQLHlGQUF5RixDQUFDLENBQUM7U0FDaEc7UUFFRCxNQUFNLFdBQVcsR0FBRyxPQUFPLENBQUMsY0FBYyxFQUFFLENBQUM7UUFDN0MsTUFBTSxXQUFXLEdBQUcsT0FBTyxDQUFDLGNBQWMsRUFBRSxDQUFDLE1BQU0sQ0FDL0MsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxpQkFBaUIsSUFBSSxDQUFDLE9BQU8sQ0FBQywrQkFBK0IsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQzlFLE1BQU0sWUFBWSxHQUFHLElBQUksd0NBQXFCLENBQUMsV0FBVyxDQUFDLENBQUM7UUFFNUQsZ0VBQWdFO1FBQ2hFLHVCQUF1QjtRQUN2QixXQUFXLENBQUMsT0FBTyxDQUFDLFVBQVUsQ0FBQyxFQUFFO1lBQy9CLE1BQU0sWUFBWSxHQUFHLGVBQVEsQ0FBQyxRQUFRLEVBQUUsVUFBVSxDQUFDLFFBQVEsQ0FBQyxDQUFDO1lBRTdELCtEQUErRDtZQUMvRCxxQ0FBcUM7WUFDckMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxHQUFHLENBQUMsWUFBWSxDQUFDLEVBQUU7Z0JBQ3BDLGFBQWEsQ0FBQyxHQUFHLENBQUMsWUFBWSxDQUFDLENBQUM7Z0JBQ2hDLFlBQVksQ0FBQyxTQUFTLENBQUMsVUFBVSxDQUFDLENBQUM7YUFDcEM7UUFDSCxDQUFDLENBQUMsQ0FBQztRQUVILElBQUksWUFBWSxDQUFDLGVBQWUsQ0FBQyxJQUFJLEtBQUssQ0FBQyxFQUFFO1lBQzNDLE9BQU8sSUFBSSxDQUFDO1NBQ2I7UUFFRCxPQUFPLEVBQUMsT0FBTyxFQUFFLElBQUksRUFBRSxZQUFZLEVBQUUsV0FBVyxFQUFFLFFBQVEsRUFBRSxZQUFZLEVBQUUsV0FBVyxFQUFDLENBQUM7SUFDekYsQ0FBQztJQUVMOzs7OztPQUtHO0lBQ0gsU0FBZSx1QkFBdUIsQ0FDbEMsSUFBVSxFQUFFLE9BQXdCLEVBQUUsZ0JBQW1DLEVBQ3pFLE1BQXlCOztZQUMzQixNQUFNLEVBQUMsV0FBVyxFQUFFLFdBQVcsRUFBRSxJQUFJLEVBQUUsWUFBWSxFQUFFLFlBQVksRUFBRSxRQUFRLEVBQUMsR0FBRyxPQUFPLENBQUM7WUFDdkYsTUFBTSxPQUFPLEdBQUcsRUFBRSxDQUFDLGFBQWEsRUFBRSxDQUFDO1lBQ25DLE1BQU0sZUFBZSxHQUFhLEVBQUUsQ0FBQztZQUNyQyxNQUFNLGVBQWUsR0FBRyxJQUFJLGtEQUEwQixDQUFDLFdBQVcsQ0FBQyxDQUFDO1lBRXBFLDZFQUE2RTtZQUM3RSxpRkFBaUY7WUFDakYsSUFBSSxnQkFBZ0IsS0FBSyxpQkFBaUIsQ0FBQyxLQUFLLEVBQUU7Z0JBQ2hELFdBQVcsQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxlQUFlLENBQUMsU0FBUyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7YUFDeEQ7WUFFRCxNQUFNLEVBQUMsZUFBZSxFQUFFLGFBQWEsRUFBQyxHQUFHLFlBQVksQ0FBQztZQUN0RCxNQUFNLEVBQUMsaUJBQWlCLEVBQUMsR0FBRyxlQUFlLENBQUM7WUFFNUMsSUFBSSxnQkFBZ0IsS0FBSyxpQkFBaUIsQ0FBQyxLQUFLLEVBQUU7Z0JBQ2hELHVGQUF1RjtnQkFDdkYsd0ZBQXdGO2dCQUN4RixpQkFBaUIsQ0FBQyxPQUFPLENBQUMsUUFBUSxDQUFDLEVBQUU7b0JBQ25DLElBQUksYUFBYSxDQUFDLEdBQUcsQ0FBQyxRQUFRLENBQUMsU0FBUyxDQUFDLEVBQUU7d0JBQ3pDLGFBQWEsQ0FBQyxHQUFHLENBQUMsUUFBUSxDQUFDLFNBQVMsQ0FBRyxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUM7cUJBQzdEO2dCQUNILENBQUMsQ0FBQyxDQUFDO2FBQ0o7WUFFRCxJQUFJLFFBQXdCLENBQUM7WUFDN0IsSUFBSSxnQkFBZ0IsS0FBSyxpQkFBaUIsQ0FBQyxLQUFLLEVBQUU7Z0JBQ2hELFFBQVEsR0FBRyxJQUFJLG1DQUFrQixDQUFDLGFBQWEsRUFBRSxXQUFXLENBQUMsQ0FBQzthQUMvRDtpQkFBTSxJQUFJLGdCQUFnQixLQUFLLGlCQUFpQixDQUFDLEtBQUssRUFBRTtnQkFDdkQsUUFBUSxHQUFHLElBQUksaUNBQWlCLEVBQUUsQ0FBQzthQUNwQztpQkFBTTtnQkFDTCxRQUFRLEdBQUcsSUFBSSx5Q0FBcUIsQ0FBQyxZQUFZLEVBQUUsYUFBYSxFQUFFLElBQUksQ0FBQyxDQUFDO2FBQ3pFO1lBRUQsSUFBSTtnQkFDRixRQUFRLENBQUMsS0FBSyxFQUFFLENBQUM7YUFDbEI7WUFBQyxPQUFPLENBQUMsRUFBRTtnQkFDVixJQUFJLGdCQUFnQixLQUFLLGlCQUFpQixDQUFDLFFBQVEsRUFBRTtvQkFDbkQsTUFBTSxDQUFDLElBQUksQ0FDUCw4REFBOEQ7d0JBQzlELDRFQUE0RTt3QkFDNUUsdUVBQXVFO3dCQUN2RSx3Q0FBd0MsQ0FBQyxDQUFDO2lCQUMvQztnQkFDRCxzRUFBc0U7Z0JBQ3RFLG9FQUFvRTtnQkFDcEUseUNBQXlDO2dCQUN6QyxNQUFNLENBQUMsSUFBSSxDQUNQLDJDQUEyQyxPQUFPLENBQUMsWUFBWSxTQUFTO29CQUN4RSxzQ0FBc0MsQ0FBQyxDQUFDO2dCQUM1QyxNQUFNLENBQUMsS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFDLFFBQVEsRUFBRSxJQUFJLENBQUMsQ0FBQztnQkFDbEMsTUFBTSxDQUFDLElBQUksQ0FDUCx5RkFBeUYsQ0FBQyxDQUFDO2dCQUMvRixPQUFPLEVBQUUsQ0FBQzthQUNYO1lBRUQseUVBQXlFO1lBQ3pFLDJFQUEyRTtZQUMzRSwwRUFBMEU7WUFDMUUsK0JBQStCO1lBQy9CLGVBQWUsQ0FBQyxPQUFPLENBQUMsQ0FBQyxPQUFPLEVBQUUsVUFBVSxFQUFFLEVBQUU7Z0JBQzlDLE1BQU0sWUFBWSxHQUFHLGVBQVEsQ0FBQyxRQUFRLEVBQUUsVUFBVSxDQUFDLFFBQVEsQ0FBQyxDQUFDO2dCQUM3RCxNQUFNLE1BQU0sR0FBRyxJQUFJLENBQUMsV0FBVyxDQUFDLFlBQVksQ0FBQyxDQUFDO2dCQUU5QyxtRUFBbUU7Z0JBQ25FLG1FQUFtRTtnQkFDbkUsT0FBTyxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsRUFBRTtvQkFDbEIsTUFBTSxTQUFTLEdBQUcsQ0FBQyxDQUFDLFNBQVMsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDO29CQUM5QyxNQUFNLEVBQUMsTUFBTSxFQUFFLE9BQU8sRUFBQyxHQUFHLFFBQVEsQ0FBQyxZQUFZLENBQUMsQ0FBQyxDQUFDLENBQUM7b0JBQ25ELE1BQU0sTUFBTSxHQUFHLHVDQUEyQixDQUFDLENBQUMsRUFBRSxNQUFNLEVBQUUsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxDQUFDO29CQUVqRSxJQUFJLENBQUMsTUFBTSxFQUFFO3dCQUNYLE9BQU87cUJBQ1I7b0JBRUQsTUFBTSxPQUFPLEdBQUcsT0FBTyxDQUFDLFNBQVMsQ0FBQyxFQUFFLENBQUMsUUFBUSxDQUFDLFdBQVcsRUFBRSxNQUFNLENBQUMsSUFBSSxFQUFFLFVBQVUsQ0FBQyxDQUFDO29CQUVwRix3RUFBd0U7b0JBQ3hFLHdCQUF3QjtvQkFDeEIsTUFBTSxDQUFDLE1BQU0sQ0FBQyxTQUFTLENBQUMsUUFBUSxFQUFFLEVBQUUsU0FBUyxDQUFDLFFBQVEsRUFBRSxDQUFDLENBQUM7b0JBQzFELE1BQU0sQ0FBQyxXQUFXLENBQUMsU0FBUyxDQUFDLFFBQVEsRUFBRSxFQUFFLE9BQU8sQ0FBQyxDQUFDO29CQUVsRCxJQUFJLE1BQU0sQ0FBQyxjQUFjLElBQUksT0FBTyxFQUFFO3dCQUNwQyxNQUFNLEVBQUMsSUFBSSxFQUFFLFNBQVMsRUFBQyxHQUNuQixFQUFFLENBQUMsNkJBQTZCLENBQUMsVUFBVSxFQUFFLENBQUMsQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLFFBQVEsRUFBRSxDQUFDLENBQUM7d0JBQzlFLGVBQWUsQ0FBQyxJQUFJLENBQ2hCLEdBQUcsWUFBWSxJQUFJLElBQUksR0FBRyxDQUFDLElBQUksU0FBUyxHQUFHLENBQUMsS0FBSyxNQUFNLENBQUMsY0FBYyxJQUFJLE9BQU8sRUFBRSxDQUFDLENBQUM7cUJBQzFGO2dCQUNILENBQUMsQ0FBQyxDQUFDO2dCQUVILElBQUksQ0FBQyxZQUFZLENBQUMsTUFBTSxDQUFDLENBQUM7WUFDNUIsQ0FBQyxDQUFDLENBQUM7WUFFSCxPQUFPLGVBQWUsQ0FBQztRQUN6QixDQUFDO0tBQUEiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7bG9nZ2luZ30gZnJvbSAnQGFuZ3VsYXItZGV2a2l0L2NvcmUnO1xuaW1wb3J0IHtSdWxlLCBTY2hlbWF0aWNDb250ZXh0LCBTY2hlbWF0aWNzRXhjZXB0aW9uLCBUcmVlfSBmcm9tICdAYW5ndWxhci1kZXZraXQvc2NoZW1hdGljcyc7XG5pbXBvcnQge2Rpcm5hbWUsIHJlbGF0aXZlfSBmcm9tICdwYXRoJztcbmltcG9ydCB7ZnJvbX0gZnJvbSAncnhqcyc7XG5pbXBvcnQgKiBhcyB0cyBmcm9tICd0eXBlc2NyaXB0JztcblxuaW1wb3J0IHtOZ0NvbXBvbmVudFRlbXBsYXRlVmlzaXRvcn0gZnJvbSAnLi4vLi4vdXRpbHMvbmdfY29tcG9uZW50X3RlbXBsYXRlJztcbmltcG9ydCB7Z2V0UHJvamVjdFRzQ29uZmlnUGF0aHN9IGZyb20gJy4uLy4uL3V0aWxzL3Byb2plY3RfdHNjb25maWdfcGF0aHMnO1xuaW1wb3J0IHtwYXJzZVRzY29uZmlnRmlsZX0gZnJvbSAnLi4vLi4vdXRpbHMvdHlwZXNjcmlwdC9wYXJzZV90c2NvbmZpZyc7XG5cbmltcG9ydCB7TmdRdWVyeVJlc29sdmVWaXNpdG9yfSBmcm9tICcuL2FuZ3VsYXIvbmdfcXVlcnlfdmlzaXRvcic7XG5pbXBvcnQge1F1ZXJ5VGVtcGxhdGVTdHJhdGVneX0gZnJvbSAnLi9zdHJhdGVnaWVzL3RlbXBsYXRlX3N0cmF0ZWd5L3RlbXBsYXRlX3N0cmF0ZWd5JztcbmltcG9ydCB7UXVlcnlUZXN0U3RyYXRlZ3l9IGZyb20gJy4vc3RyYXRlZ2llcy90ZXN0X3N0cmF0ZWd5L3Rlc3Rfc3RyYXRlZ3knO1xuaW1wb3J0IHtUaW1pbmdTdHJhdGVneX0gZnJvbSAnLi9zdHJhdGVnaWVzL3RpbWluZy1zdHJhdGVneSc7XG5pbXBvcnQge1F1ZXJ5VXNhZ2VTdHJhdGVneX0gZnJvbSAnLi9zdHJhdGVnaWVzL3VzYWdlX3N0cmF0ZWd5L3VzYWdlX3N0cmF0ZWd5JztcbmltcG9ydCB7Z2V0VHJhbnNmb3JtZWRRdWVyeUNhbGxFeHByfSBmcm9tICcuL3RyYW5zZm9ybSc7XG5cbmVudW0gU0VMRUNURURfU1RSQVRFR1kge1xuICBURU1QTEFURSxcbiAgVVNBR0UsXG4gIFRFU1RTLFxufVxuXG5pbnRlcmZhY2UgQW5hbHl6ZWRQcm9qZWN0IHtcbiAgcHJvZ3JhbTogdHMuUHJvZ3JhbTtcbiAgaG9zdDogdHMuQ29tcGlsZXJIb3N0O1xuICBxdWVyeVZpc2l0b3I6IE5nUXVlcnlSZXNvbHZlVmlzaXRvcjtcbiAgc291cmNlRmlsZXM6IHRzLlNvdXJjZUZpbGVbXTtcbiAgYmFzZVBhdGg6IHN0cmluZztcbiAgdHlwZUNoZWNrZXI6IHRzLlR5cGVDaGVja2VyO1xuICB0c2NvbmZpZ1BhdGg6IHN0cmluZztcbn1cblxuLyoqIEVudHJ5IHBvaW50IGZvciB0aGUgVjggc3RhdGljLXF1ZXJ5IG1pZ3JhdGlvbi4gKi9cbmV4cG9ydCBkZWZhdWx0IGZ1bmN0aW9uKCk6IFJ1bGUge1xuICByZXR1cm4gKHRyZWU6IFRyZWUsIGNvbnRleHQ6IFNjaGVtYXRpY0NvbnRleHQpID0+IHtcbiAgICAvLyBXZSBuZWVkIHRvIGNhc3QgdGhlIHJldHVybmVkIFwiT2JzZXJ2YWJsZVwiIHRvIFwiYW55XCIgYXMgdGhlcmUgaXMgYVxuICAgIC8vIFJ4SlMgdmVyc2lvbiBtaXNtYXRjaCB0aGF0IGJyZWFrcyB0aGUgVFMgY29tcGlsYXRpb24uXG4gICAgcmV0dXJuIGZyb20ocnVuTWlncmF0aW9uKHRyZWUsIGNvbnRleHQpLnRoZW4oKCkgPT4gdHJlZSkpIGFzIGFueTtcbiAgfTtcbn1cblxuLyoqIFJ1bnMgdGhlIFY4IG1pZ3JhdGlvbiBzdGF0aWMtcXVlcnkgbWlncmF0aW9uIGZvciBhbGwgZGV0ZXJtaW5lZCBUeXBlU2NyaXB0IHByb2plY3RzLiAqL1xuYXN5bmMgZnVuY3Rpb24gcnVuTWlncmF0aW9uKHRyZWU6IFRyZWUsIGNvbnRleHQ6IFNjaGVtYXRpY0NvbnRleHQpIHtcbiAgY29uc3Qge2J1aWxkUGF0aHMsIHRlc3RQYXRoc30gPSBnZXRQcm9qZWN0VHNDb25maWdQYXRocyh0cmVlKTtcbiAgY29uc3QgYmFzZVBhdGggPSBwcm9jZXNzLmN3ZCgpO1xuICBjb25zdCBsb2dnZXIgPSBjb250ZXh0LmxvZ2dlcjtcblxuICBsb2dnZXIuaW5mbygnLS0tLS0tIFN0YXRpYyBRdWVyeSBNaWdyYXRpb24gLS0tLS0tJyk7XG4gIGxvZ2dlci5pbmZvKCdXaXRoIEFuZ3VsYXIgdmVyc2lvbiA4LCBkZXZlbG9wZXJzIG5lZWQgdG8nKTtcbiAgbG9nZ2VyLmluZm8oJ2V4cGxpY2l0bHkgc3BlY2lmeSB0aGUgdGltaW5nIG9mIFZpZXdDaGlsZCBhbmQnKTtcbiAgbG9nZ2VyLmluZm8oJ0NvbnRlbnRDaGlsZCBxdWVyaWVzLiBSZWFkIG1vcmUgYWJvdXQgdGhpcyBoZXJlOicpO1xuICBsb2dnZXIuaW5mbygnaHR0cHM6Ly92OC5hbmd1bGFyLmlvL2d1aWRlL3N0YXRpYy1xdWVyeS1taWdyYXRpb24nKTtcblxuICBpZiAoIWJ1aWxkUGF0aHMubGVuZ3RoICYmICF0ZXN0UGF0aHMubGVuZ3RoKSB7XG4gICAgdGhyb3cgbmV3IFNjaGVtYXRpY3NFeGNlcHRpb24oXG4gICAgICAgICdDb3VsZCBub3QgZmluZCBhbnkgdHNjb25maWcgZmlsZS4gQ2Fubm90IG1pZ3JhdGUgcXVlcmllcyAnICtcbiAgICAgICAgJ3RvIGFkZCBzdGF0aWMgZmxhZy4nKTtcbiAgfVxuXG4gIGNvbnN0IGFuYWx5emVkRmlsZXMgPSBuZXcgU2V0PHN0cmluZz4oKTtcbiAgY29uc3QgYnVpbGRQcm9qZWN0cyA9IG5ldyBTZXQ8QW5hbHl6ZWRQcm9qZWN0PigpO1xuICBjb25zdCBmYWlsdXJlcyA9IFtdO1xuICBjb25zdCBzdHJhdGVneSA9IHByb2Nlc3MuZW52WydOR19TVEFUSUNfUVVFUllfVVNBR0VfU1RSQVRFR1knXSA9PT0gJ3RydWUnID9cbiAgICAgIFNFTEVDVEVEX1NUUkFURUdZLlVTQUdFIDpcbiAgICAgIFNFTEVDVEVEX1NUUkFURUdZLlRFTVBMQVRFO1xuXG4gIGZvciAoY29uc3QgdHNjb25maWdQYXRoIG9mIGJ1aWxkUGF0aHMpIHtcbiAgICBjb25zdCBwcm9qZWN0ID0gYW5hbHl6ZVByb2plY3QodHJlZSwgdHNjb25maWdQYXRoLCBiYXNlUGF0aCwgYW5hbHl6ZWRGaWxlcywgbG9nZ2VyKTtcbiAgICBpZiAocHJvamVjdCkge1xuICAgICAgYnVpbGRQcm9qZWN0cy5hZGQocHJvamVjdCk7XG4gICAgfVxuICB9XG5cbiAgaWYgKGJ1aWxkUHJvamVjdHMuc2l6ZSkge1xuICAgIGZvciAobGV0IHByb2plY3Qgb2YgQXJyYXkuZnJvbShidWlsZFByb2plY3RzLnZhbHVlcygpKSkge1xuICAgICAgZmFpbHVyZXMucHVzaCguLi5hd2FpdCBydW5TdGF0aWNRdWVyeU1pZ3JhdGlvbih0cmVlLCBwcm9qZWN0LCBzdHJhdGVneSwgbG9nZ2VyKSk7XG4gICAgfVxuICB9XG5cbiAgLy8gRm9yIHRoZSBcInRlc3RcIiB0c2NvbmZpZyBwcm9qZWN0cyB3ZSBhbHdheXMgd2FudCB0byB1c2UgdGhlIHRlc3Qgc3RyYXRlZ3kgYXNcbiAgLy8gd2UgY2FuJ3QgZGV0ZWN0IHRoZSBwcm9wZXIgdGltaW5nIHdpdGhpbiBzcGVjIGZpbGVzLlxuICBmb3IgKGNvbnN0IHRzY29uZmlnUGF0aCBvZiB0ZXN0UGF0aHMpIHtcbiAgICBjb25zdCBwcm9qZWN0ID0gYXdhaXQgYW5hbHl6ZVByb2plY3QodHJlZSwgdHNjb25maWdQYXRoLCBiYXNlUGF0aCwgYW5hbHl6ZWRGaWxlcywgbG9nZ2VyKTtcbiAgICBpZiAocHJvamVjdCkge1xuICAgICAgZmFpbHVyZXMucHVzaChcbiAgICAgICAgICAuLi5hd2FpdCBydW5TdGF0aWNRdWVyeU1pZ3JhdGlvbih0cmVlLCBwcm9qZWN0LCBTRUxFQ1RFRF9TVFJBVEVHWS5URVNUUywgbG9nZ2VyKSk7XG4gICAgfVxuICB9XG5cbiAgaWYgKGZhaWx1cmVzLmxlbmd0aCkge1xuICAgIGxvZ2dlci5pbmZvKCcnKTtcbiAgICBsb2dnZXIuaW5mbygnU29tZSBxdWVyaWVzIGNvdWxkIG5vdCBiZSBtaWdyYXRlZCBhdXRvbWF0aWNhbGx5LiBQbGVhc2UgZ28nKTtcbiAgICBsb2dnZXIuaW5mbygndGhyb3VnaCB0aGVzZSBtYW51YWxseSBhbmQgYXBwbHkgdGhlIGFwcHJvcHJpYXRlIHRpbWluZy4nKTtcbiAgICBsb2dnZXIuaW5mbygnRm9yIG1vcmUgaW5mbyBvbiBob3cgdG8gY2hvb3NlIGEgZmxhZywgcGxlYXNlIHNlZTogJyk7XG4gICAgbG9nZ2VyLmluZm8oJ2h0dHBzOi8vdjguYW5ndWxhci5pby9ndWlkZS9zdGF0aWMtcXVlcnktbWlncmF0aW9uJyk7XG4gICAgZmFpbHVyZXMuZm9yRWFjaChmYWlsdXJlID0+IGxvZ2dlci53YXJuKGDirpEgICAke2ZhaWx1cmV9YCkpO1xuICB9XG5cbiAgbG9nZ2VyLmluZm8oJy0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLScpO1xufVxuXG4vKipcbiAqIEFuYWx5emVzIHRoZSBnaXZlbiBUeXBlU2NyaXB0IHByb2plY3QgYnkgbG9va2luZyBmb3IgcXVlcmllcyB0aGF0IG5lZWQgdG8gYmVcbiAqIG1pZ3JhdGVkLiBJbiBjYXNlIHRoZXJlIGFyZSBubyBxdWVyaWVzIHRoYXQgY2FuIGJlIG1pZ3JhdGVkLCBudWxsIGlzIHJldHVybmVkLlxuICovXG5mdW5jdGlvbiBhbmFseXplUHJvamVjdChcbiAgICB0cmVlOiBUcmVlLCB0c2NvbmZpZ1BhdGg6IHN0cmluZywgYmFzZVBhdGg6IHN0cmluZywgYW5hbHl6ZWRGaWxlczogU2V0PHN0cmluZz4sXG4gICAgbG9nZ2VyOiBsb2dnaW5nLkxvZ2dlckFwaSk6XG4gICAgQW5hbHl6ZWRQcm9qZWN0fG51bGwge1xuICAgICAgY29uc3QgcGFyc2VkID0gcGFyc2VUc2NvbmZpZ0ZpbGUodHNjb25maWdQYXRoLCBkaXJuYW1lKHRzY29uZmlnUGF0aCkpO1xuICAgICAgY29uc3QgaG9zdCA9IHRzLmNyZWF0ZUNvbXBpbGVySG9zdChwYXJzZWQub3B0aW9ucywgdHJ1ZSk7XG5cbiAgICAgIC8vIFdlIG5lZWQgdG8gb3ZlcndyaXRlIHRoZSBob3N0IFwicmVhZEZpbGVcIiBtZXRob2QsIGFzIHdlIHdhbnQgdGhlIFR5cGVTY3JpcHRcbiAgICAgIC8vIHByb2dyYW0gdG8gYmUgYmFzZWQgb24gdGhlIGZpbGUgY29udGVudHMgaW4gdGhlIHZpcnR1YWwgZmlsZSB0cmVlLiBPdGhlcndpc2VcbiAgICAgIC8vIGlmIHdlIHJ1biB0aGUgbWlncmF0aW9uIGZvciBtdWx0aXBsZSB0c2NvbmZpZyBmaWxlcyB3aGljaCBoYXZlIGludGVyc2VjdGluZ1xuICAgICAgLy8gc291cmNlIGZpbGVzLCBpdCBjYW4gZW5kIHVwIHVwZGF0aW5nIHF1ZXJ5IGRlZmluaXRpb25zIG11bHRpcGxlIHRpbWVzLlxuICAgICAgaG9zdC5yZWFkRmlsZSA9IGZpbGVOYW1lID0+IHtcbiAgICAgICAgY29uc3QgYnVmZmVyID0gdHJlZS5yZWFkKHJlbGF0aXZlKGJhc2VQYXRoLCBmaWxlTmFtZSkpO1xuICAgICAgICAvLyBTdHJpcCBCT00gYXMgb3RoZXJ3aXNlIFRTQyBtZXRob2RzIChFeDogZ2V0V2lkdGgpIHdpbGwgcmV0dXJuIGFuIG9mZnNldCB3aGljaFxuICAgICAgICAvLyB3aGljaCBicmVha3MgdGhlIENMSSBVcGRhdGVSZWNvcmRlci5cbiAgICAgICAgLy8gU2VlOiBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyL3B1bGwvMzA3MTlcbiAgICAgICAgcmV0dXJuIGJ1ZmZlciA/IGJ1ZmZlci50b1N0cmluZygpLnJlcGxhY2UoL15cXHVGRUZGLywgJycpIDogdW5kZWZpbmVkO1xuICAgICAgfTtcblxuICAgICAgY29uc3QgcHJvZ3JhbSA9IHRzLmNyZWF0ZVByb2dyYW0ocGFyc2VkLmZpbGVOYW1lcywgcGFyc2VkLm9wdGlvbnMsIGhvc3QpO1xuICAgICAgY29uc3Qgc3ludGFjdGljRGlhZ25vc3RpY3MgPSBwcm9ncmFtLmdldFN5bnRhY3RpY0RpYWdub3N0aWNzKCk7XG5cbiAgICAgIC8vIFN5bnRhY3RpYyBUeXBlU2NyaXB0IGVycm9ycyBjYW4gdGhyb3cgb2ZmIHRoZSBxdWVyeSBhbmFseXNpcyBhbmQgdGhlcmVmb3JlIHdlIHdhbnRcbiAgICAgIC8vIHRvIG5vdGlmeSB0aGUgZGV2ZWxvcGVyIHRoYXQgd2UgY291bGRuJ3QgYW5hbHl6ZSBwYXJ0cyBvZiB0aGUgcHJvamVjdC4gRGV2ZWxvcGVyc1xuICAgICAgLy8gY2FuIGp1c3QgcmUtcnVuIHRoZSBtaWdyYXRpb24gYWZ0ZXIgZml4aW5nIHRoZXNlIGZhaWx1cmVzLlxuICAgICAgaWYgKHN5bnRhY3RpY0RpYWdub3N0aWNzLmxlbmd0aCkge1xuICAgICAgICBsb2dnZXIud2FybihcbiAgICAgICAgICAgIGBcXG5UeXBlU2NyaXB0IHByb2plY3QgXCIke3RzY29uZmlnUGF0aH1cIiBoYXMgc3ludGFjdGljYWwgZXJyb3JzIHdoaWNoIGNvdWxkIGNhdXNlIGAgK1xuICAgICAgICAgICAgYGFuIGluY29tcGxldGUgbWlncmF0aW9uLiBQbGVhc2UgZml4IHRoZSBmb2xsb3dpbmcgZmFpbHVyZXMgYW5kIHJlcnVuIHRoZSBtaWdyYXRpb246YCk7XG4gICAgICAgIGxvZ2dlci5lcnJvcih0cy5mb3JtYXREaWFnbm9zdGljcyhzeW50YWN0aWNEaWFnbm9zdGljcywgaG9zdCkpO1xuICAgICAgICBsb2dnZXIuaW5mbyhcbiAgICAgICAgICAgICdNaWdyYXRpb24gY2FuIGJlIHJlcnVuIHdpdGg6IFwibmcgdXBkYXRlIEBhbmd1bGFyL2NvcmUgLS1mcm9tIDcgLS10byA4IC0tbWlncmF0ZS1vbmx5XCJcXG4nKTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgdHlwZUNoZWNrZXIgPSBwcm9ncmFtLmdldFR5cGVDaGVja2VyKCk7XG4gICAgICBjb25zdCBzb3VyY2VGaWxlcyA9IHByb2dyYW0uZ2V0U291cmNlRmlsZXMoKS5maWx0ZXIoXG4gICAgICAgICAgZiA9PiAhZi5pc0RlY2xhcmF0aW9uRmlsZSAmJiAhcHJvZ3JhbS5pc1NvdXJjZUZpbGVGcm9tRXh0ZXJuYWxMaWJyYXJ5KGYpKTtcbiAgICAgIGNvbnN0IHF1ZXJ5VmlzaXRvciA9IG5ldyBOZ1F1ZXJ5UmVzb2x2ZVZpc2l0b3IodHlwZUNoZWNrZXIpO1xuXG4gICAgICAvLyBBbmFseXplIGFsbCBwcm9qZWN0IHNvdXJjZS1maWxlcyBhbmQgY29sbGVjdCBhbGwgcXVlcmllcyB0aGF0XG4gICAgICAvLyBuZWVkIHRvIGJlIG1pZ3JhdGVkLlxuICAgICAgc291cmNlRmlsZXMuZm9yRWFjaChzb3VyY2VGaWxlID0+IHtcbiAgICAgICAgY29uc3QgcmVsYXRpdmVQYXRoID0gcmVsYXRpdmUoYmFzZVBhdGgsIHNvdXJjZUZpbGUuZmlsZU5hbWUpO1xuXG4gICAgICAgIC8vIE9ubHkgbG9vayBmb3IgcXVlcmllcyB3aXRoaW4gdGhlIGN1cnJlbnQgc291cmNlIGZpbGVzIGlmIHRoZVxuICAgICAgICAvLyBmaWxlIGhhcyBub3QgYmVlbiBhbmFseXplZCBiZWZvcmUuXG4gICAgICAgIGlmICghYW5hbHl6ZWRGaWxlcy5oYXMocmVsYXRpdmVQYXRoKSkge1xuICAgICAgICAgIGFuYWx5emVkRmlsZXMuYWRkKHJlbGF0aXZlUGF0aCk7XG4gICAgICAgICAgcXVlcnlWaXNpdG9yLnZpc2l0Tm9kZShzb3VyY2VGaWxlKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICAgIGlmIChxdWVyeVZpc2l0b3IucmVzb2x2ZWRRdWVyaWVzLnNpemUgPT09IDApIHtcbiAgICAgICAgcmV0dXJuIG51bGw7XG4gICAgICB9XG5cbiAgICAgIHJldHVybiB7cHJvZ3JhbSwgaG9zdCwgdHNjb25maWdQYXRoLCB0eXBlQ2hlY2tlciwgYmFzZVBhdGgsIHF1ZXJ5VmlzaXRvciwgc291cmNlRmlsZXN9O1xuICAgIH1cblxuLyoqXG4gKiBSdW5zIHRoZSBzdGF0aWMgcXVlcnkgbWlncmF0aW9uIGZvciB0aGUgZ2l2ZW4gcHJvamVjdC4gVGhlIHNjaGVtYXRpYyBhbmFseXplcyBhbGxcbiAqIHF1ZXJpZXMgd2l0aGluIHRoZSBwcm9qZWN0IGFuZCBzZXRzIHVwIHRoZSBxdWVyeSB0aW1pbmcgYmFzZWQgb24gdGhlIGN1cnJlbnQgdXNhZ2VcbiAqIG9mIHRoZSBxdWVyeSBwcm9wZXJ0eS4gZS5nLiBhIHZpZXcgcXVlcnkgdGhhdCBpcyBub3QgdXNlZCBpbiBhbnkgbGlmZWN5Y2xlIGhvb2sgZG9lc1xuICogbm90IG5lZWQgdG8gYmUgc3RhdGljIGFuZCBjYW4gYmUgc2V0IHVwIHdpdGggXCJzdGF0aWM6IGZhbHNlXCIuXG4gKi9cbmFzeW5jIGZ1bmN0aW9uIHJ1blN0YXRpY1F1ZXJ5TWlncmF0aW9uKFxuICAgIHRyZWU6IFRyZWUsIHByb2plY3Q6IEFuYWx5emVkUHJvamVjdCwgc2VsZWN0ZWRTdHJhdGVneTogU0VMRUNURURfU1RSQVRFR1ksXG4gICAgbG9nZ2VyOiBsb2dnaW5nLkxvZ2dlckFwaSk6IFByb21pc2U8c3RyaW5nW10+IHtcbiAgY29uc3Qge3NvdXJjZUZpbGVzLCB0eXBlQ2hlY2tlciwgaG9zdCwgcXVlcnlWaXNpdG9yLCB0c2NvbmZpZ1BhdGgsIGJhc2VQYXRofSA9IHByb2plY3Q7XG4gIGNvbnN0IHByaW50ZXIgPSB0cy5jcmVhdGVQcmludGVyKCk7XG4gIGNvbnN0IGZhaWx1cmVNZXNzYWdlczogc3RyaW5nW10gPSBbXTtcbiAgY29uc3QgdGVtcGxhdGVWaXNpdG9yID0gbmV3IE5nQ29tcG9uZW50VGVtcGxhdGVWaXNpdG9yKHR5cGVDaGVja2VyKTtcblxuICAvLyBJZiB0aGUgXCJ1c2FnZVwiIHN0cmF0ZWd5IGlzIHNlbGVjdGVkLCB3ZSBhbHNvIG5lZWQgdG8gYWRkIHRoZSBxdWVyeSB2aXNpdG9yXG4gIC8vIHRvIHRoZSBhbmFseXNpcyB2aXNpdG9ycyBzbyB0aGF0IHF1ZXJ5IHVzYWdlIGluIHRlbXBsYXRlcyBjYW4gYmUgYWxzbyBjaGVja2VkLlxuICBpZiAoc2VsZWN0ZWRTdHJhdGVneSA9PT0gU0VMRUNURURfU1RSQVRFR1kuVVNBR0UpIHtcbiAgICBzb3VyY2VGaWxlcy5mb3JFYWNoKHMgPT4gdGVtcGxhdGVWaXNpdG9yLnZpc2l0Tm9kZShzKSk7XG4gIH1cblxuICBjb25zdCB7cmVzb2x2ZWRRdWVyaWVzLCBjbGFzc01ldGFkYXRhfSA9IHF1ZXJ5VmlzaXRvcjtcbiAgY29uc3Qge3Jlc29sdmVkVGVtcGxhdGVzfSA9IHRlbXBsYXRlVmlzaXRvcjtcblxuICBpZiAoc2VsZWN0ZWRTdHJhdGVneSA9PT0gU0VMRUNURURfU1RSQVRFR1kuVVNBR0UpIHtcbiAgICAvLyBBZGQgYWxsIHJlc29sdmVkIHRlbXBsYXRlcyB0byB0aGUgY2xhc3MgbWV0YWRhdGEgaWYgdGhlIHVzYWdlIHN0cmF0ZWd5IGlzIHVzZWQuIFRoaXNcbiAgICAvLyBpcyBuZWNlc3NhcnkgaW4gb3JkZXIgdG8gYmUgYWJsZSB0byBjaGVjayBjb21wb25lbnQgdGVtcGxhdGVzIGZvciBzdGF0aWMgcXVlcnkgdXNhZ2UuXG4gICAgcmVzb2x2ZWRUZW1wbGF0ZXMuZm9yRWFjaCh0ZW1wbGF0ZSA9PiB7XG4gICAgICBpZiAoY2xhc3NNZXRhZGF0YS5oYXModGVtcGxhdGUuY29udGFpbmVyKSkge1xuICAgICAgICBjbGFzc01ldGFkYXRhLmdldCh0ZW1wbGF0ZS5jb250YWluZXIpICEudGVtcGxhdGUgPSB0ZW1wbGF0ZTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIGxldCBzdHJhdGVneTogVGltaW5nU3RyYXRlZ3k7XG4gIGlmIChzZWxlY3RlZFN0cmF0ZWd5ID09PSBTRUxFQ1RFRF9TVFJBVEVHWS5VU0FHRSkge1xuICAgIHN0cmF0ZWd5ID0gbmV3IFF1ZXJ5VXNhZ2VTdHJhdGVneShjbGFzc01ldGFkYXRhLCB0eXBlQ2hlY2tlcik7XG4gIH0gZWxzZSBpZiAoc2VsZWN0ZWRTdHJhdGVneSA9PT0gU0VMRUNURURfU1RSQVRFR1kuVEVTVFMpIHtcbiAgICBzdHJhdGVneSA9IG5ldyBRdWVyeVRlc3RTdHJhdGVneSgpO1xuICB9IGVsc2Uge1xuICAgIHN0cmF0ZWd5ID0gbmV3IFF1ZXJ5VGVtcGxhdGVTdHJhdGVneSh0c2NvbmZpZ1BhdGgsIGNsYXNzTWV0YWRhdGEsIGhvc3QpO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBzdHJhdGVneS5zZXR1cCgpO1xuICB9IGNhdGNoIChlKSB7XG4gICAgaWYgKHNlbGVjdGVkU3RyYXRlZ3kgPT09IFNFTEVDVEVEX1NUUkFURUdZLlRFTVBMQVRFKSB7XG4gICAgICBsb2dnZXIud2FybihcbiAgICAgICAgICBgXFxuVGhlIHRlbXBsYXRlIG1pZ3JhdGlvbiBzdHJhdGVneSB1c2VzIHRoZSBBbmd1bGFyIGNvbXBpbGVyIGAgK1xuICAgICAgICAgIGBpbnRlcm5hbGx5IGFuZCB0aGVyZWZvcmUgcHJvamVjdHMgdGhhdCBubyBsb25nZXIgYnVpbGQgc3VjY2Vzc2Z1bGx5IGFmdGVyIGAgK1xuICAgICAgICAgIGB0aGUgdXBkYXRlIGNhbm5vdCB1c2UgdGhlIHRlbXBsYXRlIG1pZ3JhdGlvbiBzdHJhdGVneS4gUGxlYXNlIGVuc3VyZSBgICtcbiAgICAgICAgICBgdGhlcmUgYXJlIG5vIEFPVCBjb21waWxhdGlvbiBlcnJvcnMuXFxuYCk7XG4gICAgfVxuICAgIC8vIEluIGNhc2UgdGhlIHN0cmF0ZWd5IGNvdWxkIG5vdCBiZSBzZXQgdXAgcHJvcGVybHksIHdlIGp1c3QgZXhpdCB0aGVcbiAgICAvLyBtaWdyYXRpb24uIFdlIGRvbid0IHdhbnQgdG8gdGhyb3cgYW4gZXhjZXB0aW9uIGFzIHRoaXMgY291bGQgbWVhblxuICAgIC8vIHRoYXQgb3RoZXIgbWlncmF0aW9ucyBhcmUgaW50ZXJydXB0ZWQuXG4gICAgbG9nZ2VyLndhcm4oXG4gICAgICAgIGBDb3VsZCBub3Qgc2V0dXAgbWlncmF0aW9uIHN0cmF0ZWd5IGZvciBcIiR7cHJvamVjdC50c2NvbmZpZ1BhdGh9XCIuIFRoZSBgICtcbiAgICAgICAgYGZvbGxvd2luZyBlcnJvciBoYXMgYmVlbiByZXBvcnRlZDpcXG5gKTtcbiAgICBsb2dnZXIuZXJyb3IoYCR7ZS50b1N0cmluZygpfVxcbmApO1xuICAgIGxvZ2dlci5pbmZvKFxuICAgICAgICAnTWlncmF0aW9uIGNhbiBiZSByZXJ1biB3aXRoOiBcIm5nIHVwZGF0ZSBAYW5ndWxhci9jb3JlIC0tZnJvbSA3IC0tdG8gOCAtLW1pZ3JhdGUtb25seVwiXFxuJyk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgLy8gV2FsayB0aHJvdWdoIGFsbCBzb3VyY2UgZmlsZXMgdGhhdCBjb250YWluIHJlc29sdmVkIHF1ZXJpZXMgYW5kIHVwZGF0ZVxuICAvLyB0aGUgc291cmNlIGZpbGVzIGlmIG5lZWRlZC4gTm90ZSB0aGF0IHdlIG5lZWQgdG8gdXBkYXRlIG11bHRpcGxlIHF1ZXJpZXNcbiAgLy8gd2l0aGluIGEgc291cmNlIGZpbGUgd2l0aGluIHRoZSBzYW1lIHJlY29yZGVyIGluIG9yZGVyIHRvIG5vdCB0aHJvdyBvZmZcbiAgLy8gdGhlIFR5cGVTY3JpcHQgbm9kZSBvZmZzZXRzLlxuICByZXNvbHZlZFF1ZXJpZXMuZm9yRWFjaCgocXVlcmllcywgc291cmNlRmlsZSkgPT4ge1xuICAgIGNvbnN0IHJlbGF0aXZlUGF0aCA9IHJlbGF0aXZlKGJhc2VQYXRoLCBzb3VyY2VGaWxlLmZpbGVOYW1lKTtcbiAgICBjb25zdCB1cGRhdGUgPSB0cmVlLmJlZ2luVXBkYXRlKHJlbGF0aXZlUGF0aCk7XG5cbiAgICAvLyBDb21wdXRlIHRoZSBxdWVyeSB0aW1pbmcgZm9yIGFsbCByZXNvbHZlZCBxdWVyaWVzIGFuZCB1cGRhdGUgdGhlXG4gICAgLy8gcXVlcnkgZGVmaW5pdGlvbnMgdG8gZXhwbGljaXRseSBzZXQgdGhlIGRldGVybWluZWQgcXVlcnkgdGltaW5nLlxuICAgIHF1ZXJpZXMuZm9yRWFjaChxID0+IHtcbiAgICAgIGNvbnN0IHF1ZXJ5RXhwciA9IHEuZGVjb3JhdG9yLm5vZGUuZXhwcmVzc2lvbjtcbiAgICAgIGNvbnN0IHt0aW1pbmcsIG1lc3NhZ2V9ID0gc3RyYXRlZ3kuZGV0ZWN0VGltaW5nKHEpO1xuICAgICAgY29uc3QgcmVzdWx0ID0gZ2V0VHJhbnNmb3JtZWRRdWVyeUNhbGxFeHByKHEsIHRpbWluZywgISFtZXNzYWdlKTtcblxuICAgICAgaWYgKCFyZXN1bHQpIHtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuXG4gICAgICBjb25zdCBuZXdUZXh0ID0gcHJpbnRlci5wcmludE5vZGUodHMuRW1pdEhpbnQuVW5zcGVjaWZpZWQsIHJlc3VsdC5ub2RlLCBzb3VyY2VGaWxlKTtcblxuICAgICAgLy8gUmVwbGFjZSB0aGUgZXhpc3RpbmcgcXVlcnkgZGVjb3JhdG9yIGNhbGwgZXhwcmVzc2lvbiB3aXRoIHRoZSB1cGRhdGVkXG4gICAgICAvLyBjYWxsIGV4cHJlc3Npb24gbm9kZS5cbiAgICAgIHVwZGF0ZS5yZW1vdmUocXVlcnlFeHByLmdldFN0YXJ0KCksIHF1ZXJ5RXhwci5nZXRXaWR0aCgpKTtcbiAgICAgIHVwZGF0ZS5pbnNlcnRSaWdodChxdWVyeUV4cHIuZ2V0U3RhcnQoKSwgbmV3VGV4dCk7XG5cbiAgICAgIGlmIChyZXN1bHQuZmFpbHVyZU1lc3NhZ2UgfHwgbWVzc2FnZSkge1xuICAgICAgICBjb25zdCB7bGluZSwgY2hhcmFjdGVyfSA9XG4gICAgICAgICAgICB0cy5nZXRMaW5lQW5kQ2hhcmFjdGVyT2ZQb3NpdGlvbihzb3VyY2VGaWxlLCBxLmRlY29yYXRvci5ub2RlLmdldFN0YXJ0KCkpO1xuICAgICAgICBmYWlsdXJlTWVzc2FnZXMucHVzaChcbiAgICAgICAgICAgIGAke3JlbGF0aXZlUGF0aH1AJHtsaW5lICsgMX06JHtjaGFyYWN0ZXIgKyAxfTogJHtyZXN1bHQuZmFpbHVyZU1lc3NhZ2UgfHwgbWVzc2FnZX1gKTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIHRyZWUuY29tbWl0VXBkYXRlKHVwZGF0ZSk7XG4gIH0pO1xuXG4gIHJldHVybiBmYWlsdXJlTWVzc2FnZXM7XG59XG4iXX0=