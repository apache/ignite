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
        define("@angular/language-service/src/ts_plugin", ["require", "exports", "tslib", "typescript", "@angular/language-service/src/language_service", "@angular/language-service/src/typescript_host"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var ts = require("typescript"); // used as value, passed in by tsserver at runtime
    var language_service_1 = require("@angular/language-service/src/language_service");
    var typescript_host_1 = require("@angular/language-service/src/typescript_host");
    var projectHostMap = new WeakMap();
    function getExternalFiles(project) {
        var host = projectHostMap.get(project);
        if (host) {
            var externalFiles = host.getTemplateReferences();
            return externalFiles;
        }
    }
    exports.getExternalFiles = getExternalFiles;
    function completionToEntry(c) {
        return {
            // TODO: remove any and fix type error.
            kind: c.kind,
            name: c.name,
            sortText: c.sort,
            kindModifiers: ''
        };
    }
    function diagnosticChainToDiagnosticChain(chain) {
        return {
            messageText: chain.message,
            category: ts.DiagnosticCategory.Error,
            code: 0,
            next: chain.next ? diagnosticChainToDiagnosticChain(chain.next) : undefined
        };
    }
    function diagnosticMessageToDiagnosticMessageText(message) {
        if (typeof message === 'string') {
            return message;
        }
        return diagnosticChainToDiagnosticChain(message);
    }
    function diagnosticToDiagnostic(d, file) {
        var result = {
            file: file,
            start: d.span.start,
            length: d.span.end - d.span.start,
            messageText: diagnosticMessageToDiagnosticMessageText(d.message),
            category: ts.DiagnosticCategory.Error,
            code: 0,
            source: 'ng'
        };
        return result;
    }
    function create(info) {
        var oldLS = info.languageService;
        var proxy = Object.assign({}, oldLS);
        var logger = info.project.projectService.logger;
        function tryOperation(attempting, callback) {
            try {
                return callback();
            }
            catch (e) {
                logger.info("Failed to " + attempting + ": " + e.toString());
                logger.info("Stack trace: " + e.stack);
                return null;
            }
        }
        var serviceHost = new typescript_host_1.TypeScriptServiceHost(info.languageServiceHost, oldLS);
        var ls = language_service_1.createLanguageService(serviceHost);
        projectHostMap.set(info.project, serviceHost);
        proxy.getCompletionsAtPosition = function (fileName, position, options) {
            var base = oldLS.getCompletionsAtPosition(fileName, position, options) || {
                isGlobalCompletion: false,
                isMemberCompletion: false,
                isNewIdentifierLocation: false,
                entries: []
            };
            tryOperation('get completions', function () {
                var e_1, _a;
                var results = ls.getCompletionsAt(fileName, position);
                if (results && results.length) {
                    if (base === undefined) {
                        base = {
                            isGlobalCompletion: false,
                            isMemberCompletion: false,
                            isNewIdentifierLocation: false,
                            entries: []
                        };
                    }
                    try {
                        for (var results_1 = tslib_1.__values(results), results_1_1 = results_1.next(); !results_1_1.done; results_1_1 = results_1.next()) {
                            var entry = results_1_1.value;
                            base.entries.push(completionToEntry(entry));
                        }
                    }
                    catch (e_1_1) { e_1 = { error: e_1_1 }; }
                    finally {
                        try {
                            if (results_1_1 && !results_1_1.done && (_a = results_1.return)) _a.call(results_1);
                        }
                        finally { if (e_1) throw e_1.error; }
                    }
                }
            });
            return base;
        };
        proxy.getQuickInfoAtPosition = function (fileName, position) {
            var base = oldLS.getQuickInfoAtPosition(fileName, position);
            var ours = ls.getHoverAt(fileName, position);
            if (!ours) {
                return base;
            }
            var result = {
                kind: ts.ScriptElementKind.unknown,
                kindModifiers: ts.ScriptElementKindModifier.none,
                textSpan: {
                    start: ours.span.start,
                    length: ours.span.end - ours.span.start,
                },
                displayParts: ours.text.map(function (part) {
                    return {
                        text: part.text,
                        kind: part.language || 'angular',
                    };
                }),
                documentation: [],
            };
            if (base && base.tags) {
                result.tags = base.tags;
            }
            return result;
        };
        proxy.getSemanticDiagnostics = function (fileName) {
            var result = oldLS.getSemanticDiagnostics(fileName);
            var base = result || [];
            tryOperation('get diagnostics', function () {
                logger.info("Computing Angular semantic diagnostics...");
                var ours = ls.getDiagnostics(fileName);
                if (ours && ours.length) {
                    var file_1 = oldLS.getProgram().getSourceFile(fileName);
                    if (file_1) {
                        base.push.apply(base, ours.map(function (d) { return diagnosticToDiagnostic(d, file_1); }));
                    }
                }
            });
            return base;
        };
        proxy.getDefinitionAtPosition = function (fileName, position) {
            var base = oldLS.getDefinitionAtPosition(fileName, position);
            if (base && base.length) {
                return base;
            }
            var ours = ls.getDefinitionAt(fileName, position);
            if (ours && ours.length) {
                return ours.map(function (loc) {
                    return {
                        fileName: loc.fileName,
                        textSpan: {
                            start: loc.span.start,
                            length: loc.span.end - loc.span.start,
                        },
                        name: '',
                        kind: ts.ScriptElementKind.unknown,
                        containerName: loc.fileName,
                        containerKind: ts.ScriptElementKind.unknown,
                    };
                });
            }
        };
        proxy.getDefinitionAndBoundSpan = function (fileName, position) {
            var base = oldLS.getDefinitionAndBoundSpan(fileName, position);
            if (base && base.definitions && base.definitions.length) {
                return base;
            }
            var ours = ls.getDefinitionAt(fileName, position);
            if (ours && ours.length) {
                return {
                    definitions: ours.map(function (loc) {
                        return {
                            fileName: loc.fileName,
                            textSpan: {
                                start: loc.span.start,
                                length: loc.span.end - loc.span.start,
                            },
                            name: '',
                            kind: ts.ScriptElementKind.unknown,
                            containerName: loc.fileName,
                            containerKind: ts.ScriptElementKind.unknown,
                        };
                    }),
                    textSpan: {
                        start: ours[0].span.start,
                        length: ours[0].span.end - ours[0].span.start,
                    },
                };
            }
        };
        return proxy;
    }
    exports.create = create;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidHNfcGx1Z2luLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvbGFuZ3VhZ2Utc2VydmljZS9zcmMvdHNfcGx1Z2luLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUVILCtCQUFpQyxDQUFDLGtEQUFrRDtJQUdwRixtRkFBeUQ7SUFFekQsaUZBQXdEO0lBRXhELElBQU0sY0FBYyxHQUFHLElBQUksT0FBTyxFQUE2QyxDQUFDO0lBRWhGLFNBQWdCLGdCQUFnQixDQUFDLE9BQTJCO1FBQzFELElBQU0sSUFBSSxHQUFHLGNBQWMsQ0FBQyxHQUFHLENBQUMsT0FBTyxDQUFDLENBQUM7UUFDekMsSUFBSSxJQUFJLEVBQUU7WUFDUixJQUFNLGFBQWEsR0FBRyxJQUFJLENBQUMscUJBQXFCLEVBQUUsQ0FBQztZQUNuRCxPQUFPLGFBQWEsQ0FBQztTQUN0QjtJQUNILENBQUM7SUFORCw0Q0FNQztJQUVELFNBQVMsaUJBQWlCLENBQUMsQ0FBYTtRQUN0QyxPQUFPO1lBQ0wsdUNBQXVDO1lBQ3ZDLElBQUksRUFBRSxDQUFDLENBQUMsSUFBVztZQUNuQixJQUFJLEVBQUUsQ0FBQyxDQUFDLElBQUk7WUFDWixRQUFRLEVBQUUsQ0FBQyxDQUFDLElBQUk7WUFDaEIsYUFBYSxFQUFFLEVBQUU7U0FDbEIsQ0FBQztJQUNKLENBQUM7SUFFRCxTQUFTLGdDQUFnQyxDQUFDLEtBQTZCO1FBRXJFLE9BQU87WUFDTCxXQUFXLEVBQUUsS0FBSyxDQUFDLE9BQU87WUFDMUIsUUFBUSxFQUFFLEVBQUUsQ0FBQyxrQkFBa0IsQ0FBQyxLQUFLO1lBQ3JDLElBQUksRUFBRSxDQUFDO1lBQ1AsSUFBSSxFQUFFLEtBQUssQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLGdDQUFnQyxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsU0FBUztTQUM1RSxDQUFDO0lBQ0os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LENBQUMsSUFBSSxDQUFDLEtBQUs7Z0NBQ3JCLE1BQU0sRUFBRSxHQUFHLENBQUMsSUFBSSxDQUFDLEdBQUcsR0FBRyxHQUFHLENBQUMsSUFBSSxDQUFDLEtBQUs7NkJBQ3RDOzRCQUNELElBQUksRUFBRSxFQUFFOzRCQUNSLElBQUksRUFBRSxFQUFFLENBQUMsaUJBQWlCLENBQUMsT0FBTzs0QkFDbEMsYUFBYSxFQUFFLEdBQUcsQ0FBQyxRQUFROzRCQUMzQixhQUFhLEVBQUUsRUFBRSxDQUFDLGlCQUFpQixDQUFDLE9BQU87eUJBQzVDLENBQUM7b0JBQ0osQ0FBQyxDQUFDO29CQUNGLFFBQVEsRUFBRTt3QkFDUixLQUFLLEVBQUUsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxLQUFLO3dCQUN6QixNQUFNLEVBQUUsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxHQUFHLEdBQUcsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxLQUFLO3FCQUM5QztpQkFDRixDQUFDO2FBQ0g7UUFDSCxDQUFDLENBQUM7UUFFTixPQUFPLEtBQUssQ0FBQztJQUNmLENBQUM7SUFwSkQsd0JBb0pDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQgKiBhcyB0cyBmcm9tICd0eXBlc2NyaXB0JzsgLy8gdXNlZCBhcyB2YWx1ZSwgcGFzc2VkIGluIGJ5IHRzc2VydmVyIGF0IHJ1bnRpbWVcbmltcG9ydCAqIGFzIHRzcyBmcm9tICd0eXBlc2NyaXB0L2xpYi90c3NlcnZlcmxpYnJhcnknOyAvLyB1c2VkIGFzIHR5cGUgb25seVxuXG5pbXBvcnQge2NyZWF0ZUxhbmd1YWdlU2VydmljZX0gZnJvbSAnLi9sYW5ndWFnZV9zZXJ2aWNlJztcbmltcG9ydCB7Q29tcGxldGlvbiwgRGlhZ25vc3RpYywgRGlhZ25vc3RpY01lc3NhZ2VDaGFpbiwgTG9jYXRpb259IGZyb20gJy4vdHlwZXMnO1xuaW1wb3J0IHtUeXBlU2NyaXB0U2VydmljZUhvc3R9IGZyb20gJy4vdHlwZXNjcmlwdF9ob3N0JztcblxuY29uc3QgcHJvamVjdEhvc3RNYXAgPSBuZXcgV2Vha01hcDx0c3Muc2VydmVyLlByb2plY3QsIFR5cGVTY3JpcHRTZXJ2aWNlSG9zdD4oKTtcblxuZXhwb3J0IGZ1bmN0aW9uIGdldEV4dGVybmFsRmlsZXMocHJvamVjdDogdHNzLnNlcnZlci5Qcm9qZWN0KTogc3RyaW5nW118dW5kZWZpbmVkIHtcbiAgY29uc3QgaG9zdCA9IHByb2plY3RIb3N0TWFwLmdldChwcm9qZWN0KTtcbiAgaWYgKGhvc3QpIHtcbiAgICBjb25zdCBleHRlcm5hbEZpbGVzID0gaG9zdC5nZXRUZW1wbGF0ZVJlZmVyZW5jZXMoKTtcbiAgICByZXR1cm4gZXh0ZXJuYWxGaWxlcztcbiAgfVxufVxuXG5mdW5jdGlvbiBjb21wbGV0aW9uVG9FbnRyeShjOiBDb21wbGV0aW9uKTogdHMuQ29tcGxldGlvbkVudHJ5IHtcbiAgcmV0dXJuIHtcbiAgICAvLyBUT0RPOiByZW1vdmUgYW55IGFuZCBmaXggdHlwZSBlcnJvci5cbiAgICBraW5kOiBjLmtpbmQgYXMgYW55LFxuICAgIG5hbWU6IGMubmFtZSxcbiAgICBzb3J0VGV4dDogYy5zb3J0LFxuICAgIGtpbmRNb2RpZmllcnM6ICcnXG4gIH07XG59XG5cbmZ1bmN0aW9uIGRpYWdub3N0aWNDaGFpblRvRGlhZ25vc3RpY0NoYWluKGNoYWluOiBEaWFnbm9zdGljTWVzc2FnZUNoYWluKTpcbiAgICB0cy5EaWFnbm9zdGljTWVzc2FnZUNoYWluIHtcbiAgcmV0dXJuIHtcbiAgICBtZXNzYWdlVGV4dDogY2hhaW4ubWVzc2FnZSxcbiAgICBjYXRlZ29yeTogdHMuRGlhZ25vc3RpY0NhdGVnb3J5LkVycm9yLFxuICAgIGNvZGU6IDAsXG4gICAgbmV4dDogY2hhaW4ubmV4dCA/IGRpYWdub3N0aWNDaGFpblRvRGlhZ25vc3RpY0NoYWluKGNoYWluLm5leHQpIDogdW5kZWZpbmVkXG4gIH07XG59XG5cbmZ1bmN0aW9uIGRpYWdub3N0aWNNZXNzYWdlVG9EaWFnbm9zdGljTWVzc2FnZVRleHQobWVzc2FnZTogc3RyaW5nIHwgRGlhZ25vc3RpY01lc3NhZ2VDaGFpbik6IHN0cmluZ3xcbiAgICB0cy5EaWFnbm9zdGljTWVzc2FnZUNoYWluIHtcbiAgaWYgKHR5cGVvZiBtZXNzYWdlID09PSAnc3RyaW5nJykge1xuICAgIHJldHVybiBtZXNzYWdlO1xuICB9XG4gIHJldHVybiBkaWFnbm9zdGljQ2hhaW5Ub0RpYWdub3N0aWNDaGFpbihtZXNzYWdlKTtcbn1cblxuZnVuY3Rpb24gZGlhZ25vc3RpY1RvRGlhZ25vc3RpYyhkOiBEaWFnbm9zdGljLCBmaWxlOiB0cy5Tb3VyY2VGaWxlKTogdHMuRGlhZ25vc3RpYyB7XG4gIGNvbnN0IHJlc3VsdCA9IHtcbiAgICBmaWxlLFxuICAgIHN0YXJ0OiBkLnNwYW4uc3RhcnQsXG4gICAgbGVuZ3RoOiBkLnNwYW4uZW5kIC0gZC5zcGFuLnN0YXJ0LFxuICAgIG1lc3NhZ2VUZXh0OiBkaWFnbm9zdGljTWVzc2FnZVRvRGlhZ25vc3RpY01lc3NhZ2VUZXh0KGQubWVzc2FnZSksXG4gICAgY2F0ZWdvcnk6IHRzLkRpYWdub3N0aWNDYXRlZ29yeS5FcnJvcixcbiAgICBjb2RlOiAwLFxuICAgIHNvdXJjZTogJ25nJ1xuICB9O1xuICByZXR1cm4gcmVzdWx0O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlKGluZm86IHRzcy5zZXJ2ZXIuUGx1Z2luQ3JlYXRlSW5mbyk6IHRzLkxhbmd1YWdlU2VydmljZSB7XG4gIGNvbnN0IG9sZExTOiB0cy5MYW5ndWFnZVNlcnZpY2UgPSBpbmZvLmxhbmd1YWdlU2VydmljZTtcbiAgY29uc3QgcHJveHk6IHRzLkxhbmd1YWdlU2VydmljZSA9IE9iamVjdC5hc3NpZ24oe30sIG9sZExTKTtcbiAgY29uc3QgbG9nZ2VyID0gaW5mby5wcm9qZWN0LnByb2plY3RTZXJ2aWNlLmxvZ2dlcjtcblxuICBmdW5jdGlvbiB0cnlPcGVyYXRpb248VD4oYXR0ZW1wdGluZzogc3RyaW5nLCBjYWxsYmFjazogKCkgPT4gVCk6IFR8bnVsbCB7XG4gICAgdHJ5IHtcbiAgICAgIHJldHVybiBjYWxsYmFjaygpO1xuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgIGxvZ2dlci5pbmZvKGBGYWlsZWQgdG8gJHthdHRlbXB0aW5nfTogJHtlLnRvU3RyaW5nKCl9YCk7XG4gICAgICBsb2dnZXIuaW5mbyhgU3RhY2sgdHJhY2U6ICR7ZS5zdGFja31gKTtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgfVxuXG4gIGNvbnN0IHNlcnZpY2VIb3N0ID0gbmV3IFR5cGVTY3JpcHRTZXJ2aWNlSG9zdChpbmZvLmxhbmd1YWdlU2VydmljZUhvc3QsIG9sZExTKTtcbiAgY29uc3QgbHMgPSBjcmVhdGVMYW5ndWFnZVNlcnZpY2Uoc2VydmljZUhvc3QpO1xuICBwcm9qZWN0SG9zdE1hcC5zZXQoaW5mby5wcm9qZWN0LCBzZXJ2aWNlSG9zdCk7XG5cbiAgcHJveHkuZ2V0Q29tcGxldGlvbnNBdFBvc2l0aW9uID0gZnVuY3Rpb24oXG4gICAgICBmaWxlTmFtZTogc3RyaW5nLCBwb3NpdGlvbjogbnVtYmVyLCBvcHRpb25zOiB0cy5HZXRDb21wbGV0aW9uc0F0UG9zaXRpb25PcHRpb25zfHVuZGVmaW5lZCkge1xuICAgIGxldCBiYXNlID0gb2xkTFMuZ2V0Q29tcGxldGlvbnNBdFBvc2l0aW9uKGZpbGVOYW1lLCBwb3NpdGlvbiwgb3B0aW9ucykgfHwge1xuICAgICAgaXNHbG9iYWxDb21wbGV0aW9uOiBmYWxzZSxcbiAgICAgIGlzTWVtYmVyQ29tcGxldGlvbjogZmFsc2UsXG4gICAgICBpc05ld0lkZW50aWZpZXJMb2NhdGlvbjogZmFsc2UsXG4gICAgICBlbnRyaWVzOiBbXVxuICAgIH07XG4gICAgdHJ5T3BlcmF0aW9uKCdnZXQgY29tcGxldGlvbnMnLCAoKSA9PiB7XG4gICAgICBjb25zdCByZXN1bHRzID0gbHMuZ2V0Q29tcGxldGlvbnNBdChmaWxlTmFtZSwgcG9zaXRpb24pO1xuICAgICAgaWYgKHJlc3VsdHMgJiYgcmVzdWx0cy5sZW5ndGgpIHtcbiAgICAgICAgaWYgKGJhc2UgPT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgIGJhc2UgPSB7XG4gICAgICAgICAgICBpc0dsb2JhbENvbXBsZXRpb246IGZhbHNlLFxuICAgICAgICAgICAgaXNNZW1iZXJDb21wbGV0aW9uOiBmYWxzZSxcbiAgICAgICAgICAgIGlzTmV3SWRlbnRpZmllckxvY2F0aW9uOiBmYWxzZSxcbiAgICAgICAgICAgIGVudHJpZXM6IFtdXG4gICAgICAgICAgfTtcbiAgICAgICAgfVxuICAgICAgICBmb3IgKGNvbnN0IGVudHJ5IG9mIHJlc3VsdHMpIHtcbiAgICAgICAgICBiYXNlLmVudHJpZXMucHVzaChjb21wbGV0aW9uVG9FbnRyeShlbnRyeSkpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfSk7XG4gICAgcmV0dXJuIGJhc2U7XG4gIH07XG5cbiAgcHJveHkuZ2V0UXVpY2tJbmZvQXRQb3NpdGlvbiA9IGZ1bmN0aW9uKGZpbGVOYW1lOiBzdHJpbmcsIHBvc2l0aW9uOiBudW1iZXIpOiB0cy5RdWlja0luZm8gfFxuICAgICAgdW5kZWZpbmVkIHtcbiAgICAgICAgY29uc3QgYmFzZSA9IG9sZExTLmdldFF1aWNrSW5mb0F0UG9zaXRpb24oZmlsZU5hbWUsIHBvc2l0aW9uKTtcbiAgICAgICAgY29uc3Qgb3VycyA9IGxzLmdldEhvdmVyQXQoZmlsZU5hbWUsIHBvc2l0aW9uKTtcbiAgICAgICAgaWYgKCFvdXJzKSB7XG4gICAgICAgICAgcmV0dXJuIGJhc2U7XG4gICAgICAgIH1cbiAgICAgICAgY29uc3QgcmVzdWx0OiB0cy5RdWlja0luZm8gPSB7XG4gICAgICAgICAga2luZDogdHMuU2NyaXB0RWxlbWVudEtpbmQudW5rbm93bixcbiAgICAgICAgICBraW5kTW9kaWZpZXJzOiB0cy5TY3JpcHRFbGVtZW50S2luZE1vZGlmaWVyLm5vbmUsXG4gICAgICAgICAgdGV4dFNwYW46IHtcbiAgICAgICAgICAgIHN0YXJ0OiBvdXJzLnNwYW4uc3RhcnQsXG4gICAgICAgICAgICBsZW5ndGg6IG91cnMuc3Bhbi5lbmQgLSBvdXJzLnNwYW4uc3RhcnQsXG4gICAgICAgICAgfSxcbiAgICAgICAgICBkaXNwbGF5UGFydHM6IG91cnMudGV4dC5tYXAocGFydCA9PiB7XG4gICAgICAgICAgICByZXR1cm4ge1xuICAgICAgICAgICAgICB0ZXh0OiBwYXJ0LnRleHQsXG4gICAgICAgICAgICAgIGtpbmQ6IHBhcnQubGFuZ3VhZ2UgfHwgJ2FuZ3VsYXInLFxuICAgICAgICAgICAgfTtcbiAgICAgICAgICB9KSxcbiAgICAgICAgICBkb2N1bWVudGF0aW9uOiBbXSxcbiAgICAgICAgfTtcbiAgICAgICAgaWYgKGJhc2UgJiYgYmFzZS50YWdzKSB7XG4gICAgICAgICAgcmVzdWx0LnRhZ3MgPSBiYXNlLnRhZ3M7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHJlc3VsdDtcbiAgICAgIH07XG5cbiAgcHJveHkuZ2V0U2VtYW50aWNEaWFnbm9zdGljcyA9IGZ1bmN0aW9uKGZpbGVOYW1lOiBzdHJpbmcpIHtcbiAgICBsZXQgcmVzdWx0ID0gb2xkTFMuZ2V0U2VtYW50aWNEaWFnbm9zdGljcyhmaWxlTmFtZSk7XG4gICAgY29uc3QgYmFzZSA9IHJlc3VsdCB8fCBbXTtcbiAgICB0cnlPcGVyYXRpb24oJ2dldCBkaWFnbm9zdGljcycsICgpID0+IHtcbiAgICAgIGxvZ2dlci5pbmZvKGBDb21wdXRpbmcgQW5ndWxhciBzZW1hbnRpYyBkaWFnbm9zdGljcy4uLmApO1xuICAgICAgY29uc3Qgb3VycyA9IGxzLmdldERpYWdub3N0aWNzKGZpbGVOYW1lKTtcbiAgICAgIGlmIChvdXJzICYmIG91cnMubGVuZ3RoKSB7XG4gICAgICAgIGNvbnN0IGZpbGUgPSBvbGRMUy5nZXRQcm9ncmFtKCkgIS5nZXRTb3VyY2VGaWxlKGZpbGVOYW1lKTtcbiAgICAgICAgaWYgKGZpbGUpIHtcbiAgICAgICAgICBiYXNlLnB1c2guYXBwbHkoYmFzZSwgb3Vycy5tYXAoZCA9PiBkaWFnbm9zdGljVG9EaWFnbm9zdGljKGQsIGZpbGUpKSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9KTtcblxuICAgIHJldHVybiBiYXNlO1xuICB9O1xuXG4gIHByb3h5LmdldERlZmluaXRpb25BdFBvc2l0aW9uID0gZnVuY3Rpb24oZmlsZU5hbWU6IHN0cmluZywgcG9zaXRpb246IG51bWJlcik6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIFJlYWRvbmx5QXJyYXk8dHMuRGVmaW5pdGlvbkluZm8+fFxuICAgICAgdW5kZWZpbmVkIHtcbiAgICAgICAgY29uc3QgYmFzZSA9IG9sZExTLmdldERlZmluaXRpb25BdFBvc2l0aW9uKGZpbGVOYW1lLCBwb3NpdGlvbik7XG4gICAgICAgIGlmIChiYXNlICYmIGJhc2UubGVuZ3RoKSB7XG4gICAgICAgICAgcmV0dXJuIGJhc2U7XG4gICAgICAgIH1cbiAgICAgICAgY29uc3Qgb3VycyA9IGxzLmdldERlZmluaXRpb25BdChmaWxlTmFtZSwgcG9zaXRpb24pO1xuICAgICAgICBpZiAob3VycyAmJiBvdXJzLmxlbmd0aCkge1xuICAgICAgICAgIHJldHVybiBvdXJzLm1hcCgobG9jOiBMb2NhdGlvbikgPT4ge1xuICAgICAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICAgICAgZmlsZU5hbWU6IGxvYy5maWxlTmFtZSxcbiAgICAgICAgICAgICAgdGV4dFNwYW46IHtcbiAgICAgICAgICAgICAgICBzdGFydDogbG9jLnNwYW4uc3RhcnQsXG4gICAgICAgICAgICAgICAgbGVuZ3RoOiBsb2Muc3Bhbi5lbmQgLSBsb2Muc3Bhbi5zdGFydCxcbiAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgbmFtZTogJycsXG4gICAgICAgICAgICAgIGtpbmQ6IHRzLlNjcmlwdEVsZW1lbnRLaW5kLnVua25vd24sXG4gICAgICAgICAgICAgIGNvbnRhaW5lck5hbWU6IGxvYy5maWxlTmFtZSxcbiAgICAgICAgICAgICAgY29udGFpbmVyS2luZDogdHMuU2NyaXB0RWxlbWVudEtpbmQudW5rbm93bixcbiAgICAgICAgICAgIH07XG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH07XG5cbiAgcHJveHkuZ2V0RGVmaW5pdGlvbkFuZEJvdW5kU3BhbiA9IGZ1bmN0aW9uKGZpbGVOYW1lOiBzdHJpbmcsIHBvc2l0aW9uOiBudW1iZXIpOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRzLkRlZmluaXRpb25JbmZvQW5kQm91bmRTcGFuIHxcbiAgICAgIHVuZGVmaW5lZCB7XG4gICAgICAgIGNvbnN0IGJhc2UgPSBvbGRMUy5nZXREZWZpbml0aW9uQW5kQm91bmRTcGFuKGZpbGVOYW1lLCBwb3NpdGlvbik7XG4gICAgICAgIGlmIChiYXNlICYmIGJhc2UuZGVmaW5pdGlvbnMgJiYgYmFzZS5kZWZpbml0aW9ucy5sZW5ndGgpIHtcbiAgICAgICAgICByZXR1cm4gYmFzZTtcbiAgICAgICAgfVxuICAgICAgICBjb25zdCBvdXJzID0gbHMuZ2V0RGVmaW5pdGlvbkF0KGZpbGVOYW1lLCBwb3NpdGlvbik7XG4gICAgICAgIGlmIChvdXJzICYmIG91cnMubGVuZ3RoKSB7XG4gICAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICAgIGRlZmluaXRpb25zOiBvdXJzLm1hcCgobG9jOiBMb2NhdGlvbikgPT4ge1xuICAgICAgICAgICAgICByZXR1cm4ge1xuICAgICAgICAgICAgICAgIGZpbGVOYW1lOiBsb2MuZmlsZU5hbWUsXG4gICAgICAgICAgICAgICAgdGV4dFNwYW46IHtcbiAgICAgICAgICAgICAgICAgIHN0YXJ0OiBsb2Muc3Bhbi5zdGFydCxcbiAgICAgICAgICAgICAgICAgIGxlbmd0aDogbG9jLnNwYW4uZW5kIC0gbG9jLnNwYW4uc3RhcnQsXG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBuYW1lOiAnJyxcbiAgICAgICAgICAgICAgICBraW5kOiB0cy5TY3JpcHRFbGVtZW50S2luZC51bmtub3duLFxuICAgICAgICAgICAgICAgIGNvbnRhaW5lck5hbWU6IGxvYy5maWxlTmFtZSxcbiAgICAgICAgICAgICAgICBjb250YWluZXJLaW5kOiB0cy5TY3JpcHRFbGVtZW50S2luZC51bmtub3duLFxuICAgICAgICAgICAgICB9O1xuICAgICAgICAgICAgfSksXG4gICAgICAgICAgICB0ZXh0U3Bhbjoge1xuICAgICAgICAgICAgICBzdGFydDogb3Vyc1swXS5zcGFuLnN0YXJ0LFxuICAgICAgICAgICAgICBsZW5ndGg6IG91cnNbMF0uc3Bhbi5lbmQgLSBvdXJzWzBdLnNwYW4uc3RhcnQsXG4gICAgICAgICAgICB9LFxuICAgICAgICAgIH07XG4gICAgICAgIH1cbiAgICAgIH07XG5cbiAgcmV0dXJuIHByb3h5O1xufVxuIl19