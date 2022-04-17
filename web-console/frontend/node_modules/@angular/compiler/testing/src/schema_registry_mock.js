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
        define("@angular/compiler/testing/src/schema_registry_mock", ["require", "exports", "@angular/compiler"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var compiler_1 = require("@angular/compiler");
    var MockSchemaRegistry = /** @class */ (function () {
        function MockSchemaRegistry(existingProperties, attrPropMapping, existingElements, invalidProperties, invalidAttributes) {
            this.existingProperties = existingProperties;
            this.attrPropMapping = attrPropMapping;
            this.existingElements = existingElements;
            this.invalidProperties = invalidProperties;
            this.invalidAttributes = invalidAttributes;
        }
        MockSchemaRegistry.prototype.hasProperty = function (tagName, property, schemas) {
            var value = this.existingProperties[property];
            return value === void 0 ? true : value;
        };
        MockSchemaRegistry.prototype.hasElement = function (tagName, schemaMetas) {
            var value = this.existingElements[tagName.toLowerCase()];
            return value === void 0 ? true : value;
        };
        MockSchemaRegistry.prototype.allKnownElementNames = function () { return Object.keys(this.existingElements); };
        MockSchemaRegistry.prototype.securityContext = function (selector, property, isAttribute) {
            return compiler_1.core.SecurityContext.NONE;
        };
        MockSchemaRegistry.prototype.getMappedPropName = function (attrName) { return this.attrPropMapping[attrName] || attrName; };
        MockSchemaRegistry.prototype.getDefaultComponentElementName = function () { return 'ng-component'; };
        MockSchemaRegistry.prototype.validateProperty = function (name) {
            if (this.invalidProperties.indexOf(name) > -1) {
                return { error: true, msg: "Binding to property '" + name + "' is disallowed for security reasons" };
            }
            else {
                return { error: false };
            }
        };
        MockSchemaRegistry.prototype.validateAttribute = function (name) {
            if (this.invalidAttributes.indexOf(name) > -1) {
                return {
                    error: true,
                    msg: "Binding to attribute '" + name + "' is disallowed for security reasons"
                };
            }
            else {
                return { error: false };
            }
        };
        MockSchemaRegistry.prototype.normalizeAnimationStyleProperty = function (propName) { return propName; };
        MockSchemaRegistry.prototype.normalizeAnimationStyleValue = function (camelCaseProp, userProvidedProp, val) {
            return { error: null, value: val.toString() };
        };
        return MockSchemaRegistry;
    }());
    exports.MockSchemaRegistry = MockSchemaRegistry;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2NoZW1hX3JlZ2lzdHJ5X21vY2suanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci90ZXN0aW5nL3NyYy9zY2hlbWFfcmVnaXN0cnlfbW9jay50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILDhDQUE4RDtJQUU5RDtRQUNFLDRCQUNXLGtCQUE0QyxFQUM1QyxlQUF3QyxFQUN4QyxnQkFBMEMsRUFBUyxpQkFBZ0MsRUFDbkYsaUJBQWdDO1lBSGhDLHVCQUFrQixHQUFsQixrQkFBa0IsQ0FBMEI7WUFDNUMsb0JBQWUsR0FBZixlQUFlLENBQXlCO1lBQ3hDLHFCQUFnQixHQUFoQixnQkFBZ0IsQ0FBMEI7WUFBUyxzQkFBaUIsR0FBakIsaUJBQWlCLENBQWU7WUFDbkYsc0JBQWlCLEdBQWpCLGlCQUFpQixDQUFlO1FBQUcsQ0FBQztRQUUvQyx3Q0FBVyxHQUFYLFVBQVksT0FBZSxFQUFFLFFBQWdCLEVBQUUsT0FBOEI7WUFDM0UsSUFBTSxLQUFLLEdBQUcsSUFBSSxDQUFDLGtCQUFrQixDQUFDLFFBQVEsQ0FBQyxDQUFDO1lBQ2hELE9BQU8sS0FBSyxLQUFLLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQztRQUN6QyxDQUFDO1FBRUQsdUNBQVUsR0FBVixVQUFXLE9BQWUsRUFBRSxXQUFrQztZQUM1RCxJQUFNLEtBQUssR0FBRyxJQUFJLENBQUMsZ0JBQWdCLENBQUMsT0FBTyxDQUFDLFdBQVcsRUFBRSxDQUFDLENBQUM7WUFDM0QsT0FBTyxLQUFLLEtBQUssS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDO1FBQ3pDLENBQUM7UUFFRCxpREFBb0IsR0FBcEIsY0FBbUMsT0FBTyxNQUFNLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUUvRSw0Q0FBZSxHQUFmLFVBQWdCLFFBQWdCLEVBQUUsUUFBZ0IsRUFBRSxXQUFvQjtZQUN0RSxPQUFPLGVBQUksQ0FBQyxlQUFlLENBQUMsSUFBSSxDQUFDO1FBQ25DLENBQUM7UUFFRCw4Q0FBaUIsR0FBakIsVUFBa0IsUUFBZ0IsSUFBWSxPQUFPLElBQUksQ0FBQyxlQUFlLENBQUMsUUFBUSxDQUFDLElBQUksUUFBUSxDQUFDLENBQUMsQ0FBQztRQUVsRywyREFBOEIsR0FBOUIsY0FBMkMsT0FBTyxjQUFjLENBQUMsQ0FBQyxDQUFDO1FBRW5FLDZDQUFnQixHQUFoQixVQUFpQixJQUFZO1lBQzNCLElBQUksSUFBSSxDQUFDLGlCQUFpQixDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsRUFBRTtnQkFDN0MsT0FBTyxFQUFDLEtBQUssRUFBRSxJQUFJLEVBQUUsR0FBRyxFQUFFLDBCQUF3QixJQUFJLHlDQUFzQyxFQUFDLENBQUM7YUFDL0Y7aUJBQU07Z0JBQ0wsT0FBTyxFQUFDLEtBQUssRUFBRSxLQUFLLEVBQUMsQ0FBQzthQUN2QjtRQUNILENBQUM7UUFFRCw4Q0FBaUIsR0FBakIsVUFBa0IsSUFBWTtZQUM1QixJQUFJLElBQUksQ0FBQyxpQkFBaUIsQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxDQUFDLEVBQUU7Z0JBQzdDLE9BQU87b0JBQ0wsS0FBSyxFQUFFLElBQUk7b0JBQ1gsR0FBRyxFQUFFLDJCQUF5QixJQUFJLHlDQUFzQztpQkFDekUsQ0FBQzthQUNIO2lCQUFNO2dCQUNMLE9BQU8sRUFBQyxLQUFLLEVBQUUsS0FBSyxFQUFDLENBQUM7YUFDdkI7UUFDSCxDQUFDO1FBRUQsNERBQStCLEdBQS9CLFVBQWdDLFFBQWdCLElBQVksT0FBTyxRQUFRLENBQUMsQ0FBQyxDQUFDO1FBQzlFLHlEQUE0QixHQUE1QixVQUE2QixhQUFxQixFQUFFLGdCQUF3QixFQUFFLEdBQWtCO1lBRTlGLE9BQU8sRUFBQyxLQUFLLEVBQUUsSUFBTSxFQUFFLEtBQUssRUFBRSxHQUFHLENBQUMsUUFBUSxFQUFFLEVBQUMsQ0FBQztRQUNoRCxDQUFDO1FBQ0gseUJBQUM7SUFBRCxDQUFDLEFBbkRELElBbURDO0lBbkRZLGdEQUFrQiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtFbGVtZW50U2NoZW1hUmVnaXN0cnksIGNvcmV9IGZyb20gJ0Bhbmd1bGFyL2NvbXBpbGVyJztcblxuZXhwb3J0IGNsYXNzIE1vY2tTY2hlbWFSZWdpc3RyeSBpbXBsZW1lbnRzIEVsZW1lbnRTY2hlbWFSZWdpc3RyeSB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIGV4aXN0aW5nUHJvcGVydGllczoge1trZXk6IHN0cmluZ106IGJvb2xlYW59LFxuICAgICAgcHVibGljIGF0dHJQcm9wTWFwcGluZzoge1trZXk6IHN0cmluZ106IHN0cmluZ30sXG4gICAgICBwdWJsaWMgZXhpc3RpbmdFbGVtZW50czoge1trZXk6IHN0cmluZ106IGJvb2xlYW59LCBwdWJsaWMgaW52YWxpZFByb3BlcnRpZXM6IEFycmF5PHN0cmluZz4sXG4gICAgICBwdWJsaWMgaW52YWxpZEF0dHJpYnV0ZXM6IEFycmF5PHN0cmluZz4pIHt9XG5cbiAgaGFzUHJvcGVydHkodGFnTmFtZTogc3RyaW5nLCBwcm9wZXJ0eTogc3RyaW5nLCBzY2hlbWFzOiBjb3JlLlNjaGVtYU1ldGFkYXRhW10pOiBib29sZWFuIHtcbiAgICBjb25zdCB2YWx1ZSA9IHRoaXMuZXhpc3RpbmdQcm9wZXJ0aWVzW3Byb3BlcnR5XTtcbiAgICByZXR1cm4gdmFsdWUgPT09IHZvaWQgMCA/IHRydWUgOiB2YWx1ZTtcbiAgfVxuXG4gIGhhc0VsZW1lbnQodGFnTmFtZTogc3RyaW5nLCBzY2hlbWFNZXRhczogY29yZS5TY2hlbWFNZXRhZGF0YVtdKTogYm9vbGVhbiB7XG4gICAgY29uc3QgdmFsdWUgPSB0aGlzLmV4aXN0aW5nRWxlbWVudHNbdGFnTmFtZS50b0xvd2VyQ2FzZSgpXTtcbiAgICByZXR1cm4gdmFsdWUgPT09IHZvaWQgMCA/IHRydWUgOiB2YWx1ZTtcbiAgfVxuXG4gIGFsbEtub3duRWxlbWVudE5hbWVzKCk6IHN0cmluZ1tdIHsgcmV0dXJuIE9iamVjdC5rZXlzKHRoaXMuZXhpc3RpbmdFbGVtZW50cyk7IH1cblxuICBzZWN1cml0eUNvbnRleHQoc2VsZWN0b3I6IHN0cmluZywgcHJvcGVydHk6IHN0cmluZywgaXNBdHRyaWJ1dGU6IGJvb2xlYW4pOiBjb3JlLlNlY3VyaXR5Q29udGV4dCB7XG4gICAgcmV0dXJuIGNvcmUuU2VjdXJpdHlDb250ZXh0Lk5PTkU7XG4gIH1cblxuICBnZXRNYXBwZWRQcm9wTmFtZShhdHRyTmFtZTogc3RyaW5nKTogc3RyaW5nIHsgcmV0dXJuIHRoaXMuYXR0clByb3BNYXBwaW5nW2F0dHJOYW1lXSB8fCBhdHRyTmFtZTsgfVxuXG4gIGdldERlZmF1bHRDb21wb25lbnRFbGVtZW50TmFtZSgpOiBzdHJpbmcgeyByZXR1cm4gJ25nLWNvbXBvbmVudCc7IH1cblxuICB2YWxpZGF0ZVByb3BlcnR5KG5hbWU6IHN0cmluZyk6IHtlcnJvcjogYm9vbGVhbiwgbXNnPzogc3RyaW5nfSB7XG4gICAgaWYgKHRoaXMuaW52YWxpZFByb3BlcnRpZXMuaW5kZXhPZihuYW1lKSA+IC0xKSB7XG4gICAgICByZXR1cm4ge2Vycm9yOiB0cnVlLCBtc2c6IGBCaW5kaW5nIHRvIHByb3BlcnR5ICcke25hbWV9JyBpcyBkaXNhbGxvd2VkIGZvciBzZWN1cml0eSByZWFzb25zYH07XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiB7ZXJyb3I6IGZhbHNlfTtcbiAgICB9XG4gIH1cblxuICB2YWxpZGF0ZUF0dHJpYnV0ZShuYW1lOiBzdHJpbmcpOiB7ZXJyb3I6IGJvb2xlYW4sIG1zZz86IHN0cmluZ30ge1xuICAgIGlmICh0aGlzLmludmFsaWRBdHRyaWJ1dGVzLmluZGV4T2YobmFtZSkgPiAtMSkge1xuICAgICAgcmV0dXJuIHtcbiAgICAgICAgZXJyb3I6IHRydWUsXG4gICAgICAgIG1zZzogYEJpbmRpbmcgdG8gYXR0cmlidXRlICcke25hbWV9JyBpcyBkaXNhbGxvd2VkIGZvciBzZWN1cml0eSByZWFzb25zYFxuICAgICAgfTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHtlcnJvcjogZmFsc2V9O1xuICAgIH1cbiAgfVxuXG4gIG5vcm1hbGl6ZUFuaW1hdGlvblN0eWxlUHJvcGVydHkocHJvcE5hbWU6IHN0cmluZyk6IHN0cmluZyB7IHJldHVybiBwcm9wTmFtZTsgfVxuICBub3JtYWxpemVBbmltYXRpb25TdHlsZVZhbHVlKGNhbWVsQ2FzZVByb3A6IHN0cmluZywgdXNlclByb3ZpZGVkUHJvcDogc3RyaW5nLCB2YWw6IHN0cmluZ3xudW1iZXIpOlxuICAgICAge2Vycm9yOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmd9IHtcbiAgICByZXR1cm4ge2Vycm9yOiBudWxsICEsIHZhbHVlOiB2YWwudG9TdHJpbmcoKX07XG4gIH1cbn1cbiJdfQ==