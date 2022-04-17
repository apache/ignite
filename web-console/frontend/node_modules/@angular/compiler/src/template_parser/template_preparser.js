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
        define("@angular/compiler/src/template_parser/template_preparser", ["require", "exports", "@angular/compiler/src/ml_parser/tags"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tags_1 = require("@angular/compiler/src/ml_parser/tags");
    var NG_CONTENT_SELECT_ATTR = 'select';
    var LINK_ELEMENT = 'link';
    var LINK_STYLE_REL_ATTR = 'rel';
    var LINK_STYLE_HREF_ATTR = 'href';
    var LINK_STYLE_REL_VALUE = 'stylesheet';
    var STYLE_ELEMENT = 'style';
    var SCRIPT_ELEMENT = 'script';
    var NG_NON_BINDABLE_ATTR = 'ngNonBindable';
    var NG_PROJECT_AS = 'ngProjectAs';
    function preparseElement(ast) {
        var selectAttr = null;
        var hrefAttr = null;
        var relAttr = null;
        var nonBindable = false;
        var projectAs = '';
        ast.attrs.forEach(function (attr) {
            var lcAttrName = attr.name.toLowerCase();
            if (lcAttrName == NG_CONTENT_SELECT_ATTR) {
                selectAttr = attr.value;
            }
            else if (lcAttrName == LINK_STYLE_HREF_ATTR) {
                hrefAttr = attr.value;
            }
            else if (lcAttrName == LINK_STYLE_REL_ATTR) {
                relAttr = attr.value;
            }
            else if (attr.name == NG_NON_BINDABLE_ATTR) {
                nonBindable = true;
            }
            else if (attr.name == NG_PROJECT_AS) {
                if (attr.value.length > 0) {
                    projectAs = attr.value;
                }
            }
        });
        selectAttr = normalizeNgContentSelect(selectAttr);
        var nodeName = ast.name.toLowerCase();
        var type = PreparsedElementType.OTHER;
        if (tags_1.isNgContent(nodeName)) {
            type = PreparsedElementType.NG_CONTENT;
        }
        else if (nodeName == STYLE_ELEMENT) {
            type = PreparsedElementType.STYLE;
        }
        else if (nodeName == SCRIPT_ELEMENT) {
            type = PreparsedElementType.SCRIPT;
        }
        else if (nodeName == LINK_ELEMENT && relAttr == LINK_STYLE_REL_VALUE) {
            type = PreparsedElementType.STYLESHEET;
        }
        return new PreparsedElement(type, selectAttr, hrefAttr, nonBindable, projectAs);
    }
    exports.preparseElement = preparseElement;
    var PreparsedElementType;
    (function (PreparsedElementType) {
        PreparsedElementType[PreparsedElementType["NG_CONTENT"] = 0] = "NG_CONTENT";
        PreparsedElementType[PreparsedElementType["STYLE"] = 1] = "STYLE";
        PreparsedElementType[PreparsedElementType["STYLESHEET"] = 2] = "STYLESHEET";
        PreparsedElementType[PreparsedElementType["SCRIPT"] = 3] = "SCRIPT";
        PreparsedElementType[PreparsedElementType["OTHER"] = 4] = "OTHER";
    })(PreparsedElementType = exports.PreparsedElementType || (exports.PreparsedElementType = {}));
    var PreparsedElement = /** @class */ (function () {
        function PreparsedElement(type, selectAttr, hrefAttr, nonBindable, projectAs) {
            this.type = type;
            this.selectAttr = selectAttr;
            this.hrefAttr = hrefAttr;
            this.nonBindable = nonBindable;
            this.projectAs = projectAs;
        }
        return PreparsedElement;
    }());
    exports.PreparsedElement = PreparsedElement;
    function normalizeNgContentSelect(selectAttr) {
        if (selectAttr === null || selectAttr.length === 0) {
            return '*';
        }
        return selectAttr;
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGVtcGxhdGVfcHJlcGFyc2VyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL3RlbXBsYXRlX3BhcnNlci90ZW1wbGF0ZV9wcmVwYXJzZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7SUFHSCw2REFBOEM7SUFFOUMsSUFBTSxzQkFBc0IsR0FBRyxRQUFRLENBQUM7SUFDeEMsSUFBTSxZQUFZLEdBQUcsTUFBTSxDQUFDO0lBQzVCLElBQU0sbUJBQW1CLEdBQUcsS0FBSyxDQUFDO0lBQ2xDLElBQU0sb0JBQW9CLEdBQUcsTUFBTSxDQUFDO0lBQ3BDLElBQU0sb0JBQW9CLEdBQUcsWUFBWSxDQUFDO0lBQzFDLElBQU0sYUFBYSxHQUFHLE9BQU8sQ0FBQztJQUM5QixJQUFNLGNBQWMsR0FBRyxRQUFRLENBQUM7SUFDaEMsSUFBTSxvQkFBb0IsR0FBRyxlQUFlLENBQUM7SUFDN0MsSUFBTSxhQUFhLEdBQUcsYUFBYSxDQUFDO0lBRXBDLFNBQWdCLGVBQWUsQ0FBQyxHQUFpQjtRQUMvQyxJQUFJLFVBQVUsR0FBVyxJQUFNLENBQUM7UUFDaEMsSUFBSSxRQUFRLEdBQVcsSUFBTSxDQUFDO1FBQzlCLElBQUksT0FBTyxHQUFXLElBQU0sQ0FBQztRQUM3QixJQUFJLFdBQVcsR0FBRyxLQUFLLENBQUM7UUFDeEIsSUFBSSxTQUFTLEdBQUcsRUFBRSxDQUFDO1FBQ25CLEdBQUcsQ0FBQyxLQUFLLENBQUMsT0FBTyxDQUFDLFVBQUEsSUFBSTtZQUNwQixJQUFNLFVBQVUsR0FBRyxJQUFJLENBQUMsSUFBSSxDQUFDLFdBQVcsRUFBRSxDQUFDO1lBQzNDLElBQUksVUFBVSxJQUFJLHNCQUFzQixFQUFFO2dCQUN4QyxVQUFVLEdBQUcsSUFBSSxDQUFDLEtBQUssQ0FBQzthQUN6QjtpQkFBTSxJQUFJLFVBQVUsSUFBSSxvQkFBb0IsRUFBRTtnQkFDN0MsUUFBUSxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUM7YUFDdkI7aUJBQU0sSUFBSSxVQUFVLElBQUksbUJBQW1CLEVBQUU7Z0JBQzVDLE9BQU8sR0FBRyxJQUFJLENBQUMsS0FBSyxDQUFDO2FBQ3RCO2lCQUFNLElBQUksSUFBSSxDQUFDLElBQUksSUFBSSxvQkFBb0IsRUFBRTtnQkFDNUMsV0FBVyxHQUFHLElBQUksQ0FBQzthQUNwQjtpQkFBTSxJQUFJLElBQUksQ0FBQyxJQUFJLElBQUksYUFBYSxFQUFFO2dCQUNyQyxJQUFJLElBQUksQ0FBQyxLQUFLLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRTtvQkFDekIsU0FBUyxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUM7aUJBQ3hCO2FBQ0Y7UUFDSCxDQUFDLENBQUMsQ0FBQztRQUNILFVBQVUsR0FBRyx3QkFBd0IsQ0FBQyxVQUFVLENBQUMsQ0FBQztRQUNsRCxJQUFNLFFBQVEsR0FBRyxHQUFHLENBQUMsSUFBSSxDQUFDLFdBQVcsRUFBRSxDQUFDO1FBQ3hDLElBQUksSUFBSSxHQUFHLG9CQUFvQixDQUFDLEtBQUssQ0FBQztRQUN0QyxJQUFJLGtCQUFXLENBQUMsUUFBUSxDQUFDLEVBQUU7WUFDekIsSUFBSSxHQUFHLG9CQUFvQixDQUFDLFVBQVUsQ0FBQztTQUN4QzthQUFNLElBQUksUUFBUSxJQUFJLGFBQWEsRUFBRTtZQUNwQyxJQUFJLEdBQUcsb0JBQW9CLENBQUMsS0FBSyxDQUFDO1NBQ25DO2FBQU0sSUFBSSxRQUFRLElBQUksY0FBYyxFQUFFO1lBQ3JDLElBQUksR0FBRyxvQkFBb0IsQ0FBQyxNQUFNLENBQUM7U0FDcEM7YUFBTSxJQUFJLFFBQVEsSUFBSSxZQUFZLElBQUksT0FBTyxJQUFJLG9CQUFvQixFQUFFO1lBQ3RFLElBQUksR0FBRyxvQkFBb0IsQ0FBQyxVQUFVLENBQUM7U0FDeEM7UUFDRCxPQUFPLElBQUksZ0JBQWdCLENBQUMsSUFBSSxFQUFFLFVBQVUsRUFBRSxRQUFRLEVBQUUsV0FBVyxFQUFFLFNBQVMsQ0FBQyxDQUFDO0lBQ2xGLENBQUM7SUFuQ0QsMENBbUNDO0lBRUQsSUFBWSxvQkFNWDtJQU5ELFdBQVksb0JBQW9CO1FBQzlCLDJFQUFVLENBQUE7UUFDVixpRUFBSyxDQUFBO1FBQ0wsMkVBQVUsQ0FBQTtRQUNWLG1FQUFNLENBQUE7UUFDTixpRUFBSyxDQUFBO0lBQ1AsQ0FBQyxFQU5XLG9CQUFvQixHQUFwQiw0QkFBb0IsS0FBcEIsNEJBQW9CLFFBTS9CO0lBRUQ7UUFDRSwwQkFDVyxJQUEwQixFQUFTLFVBQWtCLEVBQVMsUUFBZ0IsRUFDOUUsV0FBb0IsRUFBUyxTQUFpQjtZQUQ5QyxTQUFJLEdBQUosSUFBSSxDQUFzQjtZQUFTLGVBQVUsR0FBVixVQUFVLENBQVE7WUFBUyxhQUFRLEdBQVIsUUFBUSxDQUFRO1lBQzlFLGdCQUFXLEdBQVgsV0FBVyxDQUFTO1lBQVMsY0FBUyxHQUFULFNBQVMsQ0FBUTtRQUFHLENBQUM7UUFDL0QsdUJBQUM7SUFBRCxDQUFDLEFBSkQsSUFJQztJQUpZLDRDQUFnQjtJQU83QixTQUFTLHdCQUF3QixDQUFDLFVBQWtCO1FBQ2xELElBQUksVUFBVSxLQUFLLElBQUksSUFBSSxVQUFVLENBQUMsTUFBTSxLQUFLLENBQUMsRUFBRTtZQUNsRCxPQUFPLEdBQUcsQ0FBQztTQUNaO1FBQ0QsT0FBTyxVQUFVLENBQUM7SUFDcEIsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0ICogYXMgaHRtbCBmcm9tICcuLi9tbF9wYXJzZXIvYXN0JztcbmltcG9ydCB7aXNOZ0NvbnRlbnR9IGZyb20gJy4uL21sX3BhcnNlci90YWdzJztcblxuY29uc3QgTkdfQ09OVEVOVF9TRUxFQ1RfQVRUUiA9ICdzZWxlY3QnO1xuY29uc3QgTElOS19FTEVNRU5UID0gJ2xpbmsnO1xuY29uc3QgTElOS19TVFlMRV9SRUxfQVRUUiA9ICdyZWwnO1xuY29uc3QgTElOS19TVFlMRV9IUkVGX0FUVFIgPSAnaHJlZic7XG5jb25zdCBMSU5LX1NUWUxFX1JFTF9WQUxVRSA9ICdzdHlsZXNoZWV0JztcbmNvbnN0IFNUWUxFX0VMRU1FTlQgPSAnc3R5bGUnO1xuY29uc3QgU0NSSVBUX0VMRU1FTlQgPSAnc2NyaXB0JztcbmNvbnN0IE5HX05PTl9CSU5EQUJMRV9BVFRSID0gJ25nTm9uQmluZGFibGUnO1xuY29uc3QgTkdfUFJPSkVDVF9BUyA9ICduZ1Byb2plY3RBcyc7XG5cbmV4cG9ydCBmdW5jdGlvbiBwcmVwYXJzZUVsZW1lbnQoYXN0OiBodG1sLkVsZW1lbnQpOiBQcmVwYXJzZWRFbGVtZW50IHtcbiAgbGV0IHNlbGVjdEF0dHI6IHN0cmluZyA9IG51bGwgITtcbiAgbGV0IGhyZWZBdHRyOiBzdHJpbmcgPSBudWxsICE7XG4gIGxldCByZWxBdHRyOiBzdHJpbmcgPSBudWxsICE7XG4gIGxldCBub25CaW5kYWJsZSA9IGZhbHNlO1xuICBsZXQgcHJvamVjdEFzID0gJyc7XG4gIGFzdC5hdHRycy5mb3JFYWNoKGF0dHIgPT4ge1xuICAgIGNvbnN0IGxjQXR0ck5hbWUgPSBhdHRyLm5hbWUudG9Mb3dlckNhc2UoKTtcbiAgICBpZiAobGNBdHRyTmFtZSA9PSBOR19DT05URU5UX1NFTEVDVF9BVFRSKSB7XG4gICAgICBzZWxlY3RBdHRyID0gYXR0ci52YWx1ZTtcbiAgICB9IGVsc2UgaWYgKGxjQXR0ck5hbWUgPT0gTElOS19TVFlMRV9IUkVGX0FUVFIpIHtcbiAgICAgIGhyZWZBdHRyID0gYXR0ci52YWx1ZTtcbiAgICB9IGVsc2UgaWYgKGxjQXR0ck5hbWUgPT0gTElOS19TVFlMRV9SRUxfQVRUUikge1xuICAgICAgcmVsQXR0ciA9IGF0dHIudmFsdWU7XG4gICAgfSBlbHNlIGlmIChhdHRyLm5hbWUgPT0gTkdfTk9OX0JJTkRBQkxFX0FUVFIpIHtcbiAgICAgIG5vbkJpbmRhYmxlID0gdHJ1ZTtcbiAgICB9IGVsc2UgaWYgKGF0dHIubmFtZSA9PSBOR19QUk9KRUNUX0FTKSB7XG4gICAgICBpZiAoYXR0ci52YWx1ZS5sZW5ndGggPiAwKSB7XG4gICAgICAgIHByb2plY3RBcyA9IGF0dHIudmFsdWU7XG4gICAgICB9XG4gICAgfVxuICB9KTtcbiAgc2VsZWN0QXR0ciA9IG5vcm1hbGl6ZU5nQ29udGVudFNlbGVjdChzZWxlY3RBdHRyKTtcbiAgY29uc3Qgbm9kZU5hbWUgPSBhc3QubmFtZS50b0xvd2VyQ2FzZSgpO1xuICBsZXQgdHlwZSA9IFByZXBhcnNlZEVsZW1lbnRUeXBlLk9USEVSO1xuICBpZiAoaXNOZ0NvbnRlbnQobm9kZU5hbWUpKSB7XG4gICAgdHlwZSA9IFByZXBhcnNlZEVsZW1lbnRUeXBlLk5HX0NPTlRFTlQ7XG4gIH0gZWxzZSBpZiAobm9kZU5hbWUgPT0gU1RZTEVfRUxFTUVOVCkge1xuICAgIHR5cGUgPSBQcmVwYXJzZWRFbGVtZW50VHlwZS5TVFlMRTtcbiAgfSBlbHNlIGlmIChub2RlTmFtZSA9PSBTQ1JJUFRfRUxFTUVOVCkge1xuICAgIHR5cGUgPSBQcmVwYXJzZWRFbGVtZW50VHlwZS5TQ1JJUFQ7XG4gIH0gZWxzZSBpZiAobm9kZU5hbWUgPT0gTElOS19FTEVNRU5UICYmIHJlbEF0dHIgPT0gTElOS19TVFlMRV9SRUxfVkFMVUUpIHtcbiAgICB0eXBlID0gUHJlcGFyc2VkRWxlbWVudFR5cGUuU1RZTEVTSEVFVDtcbiAgfVxuICByZXR1cm4gbmV3IFByZXBhcnNlZEVsZW1lbnQodHlwZSwgc2VsZWN0QXR0ciwgaHJlZkF0dHIsIG5vbkJpbmRhYmxlLCBwcm9qZWN0QXMpO1xufVxuXG5leHBvcnQgZW51bSBQcmVwYXJzZWRFbGVtZW50VHlwZSB7XG4gIE5HX0NPTlRFTlQsXG4gIFNUWUxFLFxuICBTVFlMRVNIRUVULFxuICBTQ1JJUFQsXG4gIE9USEVSXG59XG5cbmV4cG9ydCBjbGFzcyBQcmVwYXJzZWRFbGVtZW50IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgdHlwZTogUHJlcGFyc2VkRWxlbWVudFR5cGUsIHB1YmxpYyBzZWxlY3RBdHRyOiBzdHJpbmcsIHB1YmxpYyBocmVmQXR0cjogc3RyaW5nLFxuICAgICAgcHVibGljIG5vbkJpbmRhYmxlOiBib29sZWFuLCBwdWJsaWMgcHJvamVjdEFzOiBzdHJpbmcpIHt9XG59XG5cblxuZnVuY3Rpb24gbm9ybWFsaXplTmdDb250ZW50U2VsZWN0KHNlbGVjdEF0dHI6IHN0cmluZyk6IHN0cmluZyB7XG4gIGlmIChzZWxlY3RBdHRyID09PSBudWxsIHx8IHNlbGVjdEF0dHIubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuICcqJztcbiAgfVxuICByZXR1cm4gc2VsZWN0QXR0cjtcbn1cbiJdfQ==