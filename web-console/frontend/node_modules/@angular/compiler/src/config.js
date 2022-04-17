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
        define("@angular/compiler/src/config", ["require", "exports", "@angular/compiler/src/core", "@angular/compiler/src/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var core_1 = require("@angular/compiler/src/core");
    var util_1 = require("@angular/compiler/src/util");
    var CompilerConfig = /** @class */ (function () {
        function CompilerConfig(_a) {
            var _b = _a === void 0 ? {} : _a, _c = _b.defaultEncapsulation, defaultEncapsulation = _c === void 0 ? core_1.ViewEncapsulation.Emulated : _c, _d = _b.useJit, useJit = _d === void 0 ? true : _d, _e = _b.jitDevMode, jitDevMode = _e === void 0 ? false : _e, _f = _b.missingTranslation, missingTranslation = _f === void 0 ? null : _f, preserveWhitespaces = _b.preserveWhitespaces, strictInjectionParameters = _b.strictInjectionParameters;
            this.defaultEncapsulation = defaultEncapsulation;
            this.useJit = !!useJit;
            this.jitDevMode = !!jitDevMode;
            this.missingTranslation = missingTranslation;
            this.preserveWhitespaces = preserveWhitespacesDefault(util_1.noUndefined(preserveWhitespaces));
            this.strictInjectionParameters = strictInjectionParameters === true;
        }
        return CompilerConfig;
    }());
    exports.CompilerConfig = CompilerConfig;
    function preserveWhitespacesDefault(preserveWhitespacesOption, defaultSetting) {
        if (defaultSetting === void 0) { defaultSetting = false; }
        return preserveWhitespacesOption === null ? defaultSetting : preserveWhitespacesOption;
    }
    exports.preserveWhitespacesDefault = preserveWhitespacesDefault;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29uZmlnLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2NvbmZpZy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUdILG1EQUFxRTtJQUdyRSxtREFBbUM7SUFFbkM7UUFRRSx3QkFDSSxFQVFNO2dCQVJOLDRCQVFNLEVBUkwsNEJBQWlELEVBQWpELDZFQUFpRCxFQUFFLGNBQWEsRUFBYixrQ0FBYSxFQUFFLGtCQUFrQixFQUFsQix1Q0FBa0IsRUFDcEYsMEJBQXlCLEVBQXpCLDhDQUF5QixFQUFFLDRDQUFtQixFQUFFLHdEQUF5QjtZQVE1RSxJQUFJLENBQUMsb0JBQW9CLEdBQUcsb0JBQW9CLENBQUM7WUFDakQsSUFBSSxDQUFDLE1BQU0sR0FBRyxDQUFDLENBQUMsTUFBTSxDQUFDO1lBQ3ZCLElBQUksQ0FBQyxVQUFVLEdBQUcsQ0FBQyxDQUFDLFVBQVUsQ0FBQztZQUMvQixJQUFJLENBQUMsa0JBQWtCLEdBQUcsa0JBQWtCLENBQUM7WUFDN0MsSUFBSSxDQUFDLG1CQUFtQixHQUFHLDBCQUEwQixDQUFDLGtCQUFXLENBQUMsbUJBQW1CLENBQUMsQ0FBQyxDQUFDO1lBQ3hGLElBQUksQ0FBQyx5QkFBeUIsR0FBRyx5QkFBeUIsS0FBSyxJQUFJLENBQUM7UUFDdEUsQ0FBQztRQUNILHFCQUFDO0lBQUQsQ0FBQyxBQXpCRCxJQXlCQztJQXpCWSx3Q0FBYztJQTJCM0IsU0FBZ0IsMEJBQTBCLENBQ3RDLHlCQUF5QyxFQUFFLGNBQXNCO1FBQXRCLCtCQUFBLEVBQUEsc0JBQXNCO1FBQ25FLE9BQU8seUJBQXlCLEtBQUssSUFBSSxDQUFDLENBQUMsQ0FBQyxjQUFjLENBQUMsQ0FBQyxDQUFDLHlCQUF5QixDQUFDO0lBQ3pGLENBQUM7SUFIRCxnRUFHQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21waWxlSWRlbnRpZmllck1ldGFkYXRhfSBmcm9tICcuL2NvbXBpbGVfbWV0YWRhdGEnO1xuaW1wb3J0IHtNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneSwgVmlld0VuY2Fwc3VsYXRpb259IGZyb20gJy4vY29yZSc7XG5pbXBvcnQge0lkZW50aWZpZXJzfSBmcm9tICcuL2lkZW50aWZpZXJzJztcbmltcG9ydCAqIGFzIG8gZnJvbSAnLi9vdXRwdXQvb3V0cHV0X2FzdCc7XG5pbXBvcnQge25vVW5kZWZpbmVkfSBmcm9tICcuL3V0aWwnO1xuXG5leHBvcnQgY2xhc3MgQ29tcGlsZXJDb25maWcge1xuICBwdWJsaWMgZGVmYXVsdEVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9ufG51bGw7XG4gIHB1YmxpYyB1c2VKaXQ6IGJvb2xlYW47XG4gIHB1YmxpYyBqaXREZXZNb2RlOiBib29sZWFuO1xuICBwdWJsaWMgbWlzc2luZ1RyYW5zbGF0aW9uOiBNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneXxudWxsO1xuICBwdWJsaWMgcHJlc2VydmVXaGl0ZXNwYWNlczogYm9vbGVhbjtcbiAgcHVibGljIHN0cmljdEluamVjdGlvblBhcmFtZXRlcnM6IGJvb2xlYW47XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICB7ZGVmYXVsdEVuY2Fwc3VsYXRpb24gPSBWaWV3RW5jYXBzdWxhdGlvbi5FbXVsYXRlZCwgdXNlSml0ID0gdHJ1ZSwgaml0RGV2TW9kZSA9IGZhbHNlLFxuICAgICAgIG1pc3NpbmdUcmFuc2xhdGlvbiA9IG51bGwsIHByZXNlcnZlV2hpdGVzcGFjZXMsIHN0cmljdEluamVjdGlvblBhcmFtZXRlcnN9OiB7XG4gICAgICAgIGRlZmF1bHRFbmNhcHN1bGF0aW9uPzogVmlld0VuY2Fwc3VsYXRpb24sXG4gICAgICAgIHVzZUppdD86IGJvb2xlYW4sXG4gICAgICAgIGppdERldk1vZGU/OiBib29sZWFuLFxuICAgICAgICBtaXNzaW5nVHJhbnNsYXRpb24/OiBNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneXxudWxsLFxuICAgICAgICBwcmVzZXJ2ZVdoaXRlc3BhY2VzPzogYm9vbGVhbixcbiAgICAgICAgc3RyaWN0SW5qZWN0aW9uUGFyYW1ldGVycz86IGJvb2xlYW4sXG4gICAgICB9ID0ge30pIHtcbiAgICB0aGlzLmRlZmF1bHRFbmNhcHN1bGF0aW9uID0gZGVmYXVsdEVuY2Fwc3VsYXRpb247XG4gICAgdGhpcy51c2VKaXQgPSAhIXVzZUppdDtcbiAgICB0aGlzLmppdERldk1vZGUgPSAhIWppdERldk1vZGU7XG4gICAgdGhpcy5taXNzaW5nVHJhbnNsYXRpb24gPSBtaXNzaW5nVHJhbnNsYXRpb247XG4gICAgdGhpcy5wcmVzZXJ2ZVdoaXRlc3BhY2VzID0gcHJlc2VydmVXaGl0ZXNwYWNlc0RlZmF1bHQobm9VbmRlZmluZWQocHJlc2VydmVXaGl0ZXNwYWNlcykpO1xuICAgIHRoaXMuc3RyaWN0SW5qZWN0aW9uUGFyYW1ldGVycyA9IHN0cmljdEluamVjdGlvblBhcmFtZXRlcnMgPT09IHRydWU7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHByZXNlcnZlV2hpdGVzcGFjZXNEZWZhdWx0KFxuICAgIHByZXNlcnZlV2hpdGVzcGFjZXNPcHRpb246IGJvb2xlYW4gfCBudWxsLCBkZWZhdWx0U2V0dGluZyA9IGZhbHNlKTogYm9vbGVhbiB7XG4gIHJldHVybiBwcmVzZXJ2ZVdoaXRlc3BhY2VzT3B0aW9uID09PSBudWxsID8gZGVmYXVsdFNldHRpbmcgOiBwcmVzZXJ2ZVdoaXRlc3BhY2VzT3B0aW9uO1xufVxuIl19