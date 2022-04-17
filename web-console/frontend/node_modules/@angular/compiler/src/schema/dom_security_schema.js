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
        define("@angular/compiler/src/schema/dom_security_schema", ["require", "exports", "tslib", "@angular/compiler/src/core"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var core_1 = require("@angular/compiler/src/core");
    // =================================================================================================
    // =================================================================================================
    // =========== S T O P   -  S T O P   -  S T O P   -  S T O P   -  S T O P   -  S T O P  ===========
    // =================================================================================================
    // =================================================================================================
    //
    //        DO NOT EDIT THIS LIST OF SECURITY SENSITIVE PROPERTIES WITHOUT A SECURITY REVIEW!
    //                               Reach out to mprobst for details.
    //
    // =================================================================================================
    /** Map from tagName|propertyName SecurityContext. Properties applying to all tags use '*'. */
    var _SECURITY_SCHEMA;
    function SECURITY_SCHEMA() {
        if (!_SECURITY_SCHEMA) {
            _SECURITY_SCHEMA = {};
            // Case is insignificant below, all element and attribute names are lower-cased for lookup.
            registerContext(core_1.SecurityContext.HTML, [
                'iframe|srcdoc',
                '*|innerHTML',
                '*|outerHTML',
            ]);
            registerContext(core_1.SecurityContext.STYLE, ['*|style']);
            // NB: no SCRIPT contexts here, they are never allowed due to the parser stripping them.
            registerContext(core_1.SecurityContext.URL, [
                '*|formAction', 'area|href', 'area|ping', 'audio|src', 'a|href',
                'a|ping', 'blockquote|cite', 'body|background', 'del|cite', 'form|action',
                'img|src', 'img|srcset', 'input|src', 'ins|cite', 'q|cite',
                'source|src', 'source|srcset', 'track|src', 'video|poster', 'video|src',
            ]);
            registerContext(core_1.SecurityContext.RESOURCE_URL, [
                'applet|code',
                'applet|codebase',
                'base|href',
                'embed|src',
                'frame|src',
                'head|profile',
                'html|manifest',
                'iframe|src',
                'link|href',
                'media|src',
                'object|codebase',
                'object|data',
                'script|src',
            ]);
        }
        return _SECURITY_SCHEMA;
    }
    exports.SECURITY_SCHEMA = SECURITY_SCHEMA;
    function registerContext(ctx, specs) {
        var e_1, _a;
        try {
            for (var specs_1 = tslib_1.__values(specs), specs_1_1 = specs_1.next(); !specs_1_1.done; specs_1_1 = specs_1.next()) {
                var spec = specs_1_1.value;
                _SECURITY_SCHEMA[spec.toLowerCase()] = ctx;
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (specs_1_1 && !specs_1_1.done && (_a = specs_1.return)) _a.call(specs_1);
            }
            finally { if (e_1) throw e_1.error; }
        }
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZG9tX3NlY3VyaXR5X3NjaGVtYS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9zY2hlbWEvZG9tX3NlY3VyaXR5X3NjaGVtYS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFFSCxtREFBd0M7SUFFeEMsb0dBQW9HO0lBQ3BHLG9HQUFvRztJQUNwRyxvR0FBb0c7SUFDcEcsb0dBQW9HO0lBQ3BHLG9HQUFvRztJQUNwRyxFQUFFO0lBQ0YsMkZBQTJGO0lBQzNGLGtFQUFrRTtJQUNsRSxFQUFFO0lBQ0Ysb0dBQW9HO0lBRXBHLDhGQUE4RjtJQUM5RixJQUFJLGdCQUFrRCxDQUFDO0lBRXZELFNBQWdCLGVBQWU7UUFDN0IsSUFBSSxDQUFDLGdCQUFnQixFQUFFO1lBQ3JCLGdCQUFnQixHQUFHLEVBQUUsQ0FBQztZQUN0QiwyRkFBMkY7WUFFM0YsZUFBZSxDQUFDLHNCQUFlLENBQUMsSUFBSSxFQUFFO2dCQUNwQyxlQUFlO2dCQUNmLGFBQWE7Z0JBQ2IsYUFBYTthQUNkLENBQUMsQ0FBQztZQUNILGVBQWUsQ0FBQyxzQkFBZSxDQUFDLEtBQUssRUFBRSxDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUM7WUFDcEQsd0ZBQXdGO1lBQ3hGLGVBQWUsQ0FBQyxzQkFBZSxDQUFDLEdBQUcsRUFBRTtnQkFDbkMsY0FBYyxFQUFFLFdBQVcsRUFBUSxXQUFXLEVBQVEsV0FBVyxFQUFLLFFBQVE7Z0JBQzlFLFFBQVEsRUFBUSxpQkFBaUIsRUFBRSxpQkFBaUIsRUFBRSxVQUFVLEVBQU0sYUFBYTtnQkFDbkYsU0FBUyxFQUFPLFlBQVksRUFBTyxXQUFXLEVBQVEsVUFBVSxFQUFNLFFBQVE7Z0JBQzlFLFlBQVksRUFBSSxlQUFlLEVBQUksV0FBVyxFQUFRLGNBQWMsRUFBRSxXQUFXO2FBQ2xGLENBQUMsQ0FBQztZQUNILGVBQWUsQ0FBQyxzQkFBZSxDQUFDLFlBQVksRUFBRTtnQkFDNUMsYUFBYTtnQkFDYixpQkFBaUI7Z0JBQ2pCLFdBQVc7Z0JBQ1gsV0FBVztnQkFDWCxXQUFXO2dCQUNYLGNBQWM7Z0JBQ2QsZUFBZTtnQkFDZixZQUFZO2dCQUNaLFdBQVc7Z0JBQ1gsV0FBVztnQkFDWCxpQkFBaUI7Z0JBQ2pCLGFBQWE7Z0JBQ2IsWUFBWTthQUNiLENBQUMsQ0FBQztTQUNKO1FBQ0QsT0FBTyxnQkFBZ0IsQ0FBQztJQUMxQixDQUFDO0lBbkNELDBDQW1DQztJQUVELFNBQVMsZUFBZSxDQUFDLEdBQW9CLEVBQUUsS0FBZTs7O1lBQzVELEtBQW1CLElBQUEsVUFBQSxpQkFBQSxLQUFLLENBQUEsNEJBQUE7Z0JBQW5CLElBQU0sSUFBSSxrQkFBQTtnQkFBVyxnQkFBZ0IsQ0FBQyxJQUFJLENBQUMsV0FBVyxFQUFFLENBQUMsR0FBRyxHQUFHLENBQUM7YUFBQTs7Ozs7Ozs7O0lBQ3ZFLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7U2VjdXJpdHlDb250ZXh0fSBmcm9tICcuLi9jb3JlJztcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gPT09PT09PT09PT0gUyBUIE8gUCAgIC0gIFMgVCBPIFAgICAtICBTIFQgTyBQICAgLSAgUyBUIE8gUCAgIC0gIFMgVCBPIFAgICAtICBTIFQgTyBQICA9PT09PT09PT09PVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy9cbi8vICAgICAgICBETyBOT1QgRURJVCBUSElTIExJU1QgT0YgU0VDVVJJVFkgU0VOU0lUSVZFIFBST1BFUlRJRVMgV0lUSE9VVCBBIFNFQ1VSSVRZIFJFVklFVyFcbi8vICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIFJlYWNoIG91dCB0byBtcHJvYnN0IGZvciBkZXRhaWxzLlxuLy9cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cblxuLyoqIE1hcCBmcm9tIHRhZ05hbWV8cHJvcGVydHlOYW1lIFNlY3VyaXR5Q29udGV4dC4gUHJvcGVydGllcyBhcHBseWluZyB0byBhbGwgdGFncyB1c2UgJyonLiAqL1xubGV0IF9TRUNVUklUWV9TQ0hFTUEgIToge1trOiBzdHJpbmddOiBTZWN1cml0eUNvbnRleHR9O1xuXG5leHBvcnQgZnVuY3Rpb24gU0VDVVJJVFlfU0NIRU1BKCk6IHtbazogc3RyaW5nXTogU2VjdXJpdHlDb250ZXh0fSB7XG4gIGlmICghX1NFQ1VSSVRZX1NDSEVNQSkge1xuICAgIF9TRUNVUklUWV9TQ0hFTUEgPSB7fTtcbiAgICAvLyBDYXNlIGlzIGluc2lnbmlmaWNhbnQgYmVsb3csIGFsbCBlbGVtZW50IGFuZCBhdHRyaWJ1dGUgbmFtZXMgYXJlIGxvd2VyLWNhc2VkIGZvciBsb29rdXAuXG5cbiAgICByZWdpc3RlckNvbnRleHQoU2VjdXJpdHlDb250ZXh0LkhUTUwsIFtcbiAgICAgICdpZnJhbWV8c3JjZG9jJyxcbiAgICAgICcqfGlubmVySFRNTCcsXG4gICAgICAnKnxvdXRlckhUTUwnLFxuICAgIF0pO1xuICAgIHJlZ2lzdGVyQ29udGV4dChTZWN1cml0eUNvbnRleHQuU1RZTEUsIFsnKnxzdHlsZSddKTtcbiAgICAvLyBOQjogbm8gU0NSSVBUIGNvbnRleHRzIGhlcmUsIHRoZXkgYXJlIG5ldmVyIGFsbG93ZWQgZHVlIHRvIHRoZSBwYXJzZXIgc3RyaXBwaW5nIHRoZW0uXG4gICAgcmVnaXN0ZXJDb250ZXh0KFNlY3VyaXR5Q29udGV4dC5VUkwsIFtcbiAgICAgICcqfGZvcm1BY3Rpb24nLCAnYXJlYXxocmVmJywgICAgICAgJ2FyZWF8cGluZycsICAgICAgICdhdWRpb3xzcmMnLCAgICAnYXxocmVmJyxcbiAgICAgICdhfHBpbmcnLCAgICAgICAnYmxvY2txdW90ZXxjaXRlJywgJ2JvZHl8YmFja2dyb3VuZCcsICdkZWx8Y2l0ZScsICAgICAnZm9ybXxhY3Rpb24nLFxuICAgICAgJ2ltZ3xzcmMnLCAgICAgICdpbWd8c3Jjc2V0JywgICAgICAnaW5wdXR8c3JjJywgICAgICAgJ2luc3xjaXRlJywgICAgICdxfGNpdGUnLFxuICAgICAgJ3NvdXJjZXxzcmMnLCAgICdzb3VyY2V8c3Jjc2V0JywgICAndHJhY2t8c3JjJywgICAgICAgJ3ZpZGVvfHBvc3RlcicsICd2aWRlb3xzcmMnLFxuICAgIF0pO1xuICAgIHJlZ2lzdGVyQ29udGV4dChTZWN1cml0eUNvbnRleHQuUkVTT1VSQ0VfVVJMLCBbXG4gICAgICAnYXBwbGV0fGNvZGUnLFxuICAgICAgJ2FwcGxldHxjb2RlYmFzZScsXG4gICAgICAnYmFzZXxocmVmJyxcbiAgICAgICdlbWJlZHxzcmMnLFxuICAgICAgJ2ZyYW1lfHNyYycsXG4gICAgICAnaGVhZHxwcm9maWxlJyxcbiAgICAgICdodG1sfG1hbmlmZXN0JyxcbiAgICAgICdpZnJhbWV8c3JjJyxcbiAgICAgICdsaW5rfGhyZWYnLFxuICAgICAgJ21lZGlhfHNyYycsXG4gICAgICAnb2JqZWN0fGNvZGViYXNlJyxcbiAgICAgICdvYmplY3R8ZGF0YScsXG4gICAgICAnc2NyaXB0fHNyYycsXG4gICAgXSk7XG4gIH1cbiAgcmV0dXJuIF9TRUNVUklUWV9TQ0hFTUE7XG59XG5cbmZ1bmN0aW9uIHJlZ2lzdGVyQ29udGV4dChjdHg6IFNlY3VyaXR5Q29udGV4dCwgc3BlY3M6IHN0cmluZ1tdKSB7XG4gIGZvciAoY29uc3Qgc3BlYyBvZiBzcGVjcykgX1NFQ1VSSVRZX1NDSEVNQVtzcGVjLnRvTG93ZXJDYXNlKCldID0gY3R4O1xufVxuIl19