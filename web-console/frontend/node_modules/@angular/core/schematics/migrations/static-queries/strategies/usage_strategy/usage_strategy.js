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
        define("@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/usage_strategy", ["require", "exports", "typescript", "@angular/core/schematics/utils/parse_html", "@angular/core/schematics/utils/typescript/property_name", "@angular/core/schematics/migrations/static-queries/angular/query-definition", "@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/declaration_usage_visitor", "@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/super_class_context", "@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/template_usage_visitor"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const ts = require("typescript");
    const parse_html_1 = require("@angular/core/schematics/utils/parse_html");
    const property_name_1 = require("@angular/core/schematics/utils/typescript/property_name");
    const query_definition_1 = require("@angular/core/schematics/migrations/static-queries/angular/query-definition");
    const declaration_usage_visitor_1 = require("@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/declaration_usage_visitor");
    const super_class_context_1 = require("@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/super_class_context");
    const template_usage_visitor_1 = require("@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/template_usage_visitor");
    /**
     * Object that maps a given type of query to a list of lifecycle hooks that
     * could be used to access such a query statically.
     */
    const STATIC_QUERY_LIFECYCLE_HOOKS = {
        [query_definition_1.QueryType.ViewChild]: ['ngOnChanges', 'ngOnInit', 'ngDoCheck', 'ngAfterContentInit', 'ngAfterContentChecked'],
        [query_definition_1.QueryType.ContentChild]: ['ngOnChanges', 'ngOnInit', 'ngDoCheck'],
    };
    /**
     * Query timing strategy that determines the timing of a given query by inspecting how
     * the query is accessed within the project's TypeScript source files. Read more about
     * this strategy here: https://hackmd.io/s/Hymvc2OKE
     */
    class QueryUsageStrategy {
        constructor(classMetadata, typeChecker) {
            this.classMetadata = classMetadata;
            this.typeChecker = typeChecker;
        }
        setup() { }
        /**
         * Analyzes the usage of the given query and determines the query timing based
         * on the current usage of the query.
         */
        detectTiming(query) {
            if (query.property === null) {
                return { timing: null, message: 'Queries defined on accessors cannot be analyzed.' };
            }
            const usage = this.analyzeQueryUsage(query.container, query, []);
            if (usage === declaration_usage_visitor_1.ResolvedUsage.AMBIGUOUS) {
                return {
                    timing: query_definition_1.QueryTiming.STATIC,
                    message: 'Query timing is ambiguous. Please check if the query can be marked as dynamic.'
                };
            }
            else if (usage === declaration_usage_visitor_1.ResolvedUsage.SYNCHRONOUS) {
                return { timing: query_definition_1.QueryTiming.STATIC };
            }
            else {
                return { timing: query_definition_1.QueryTiming.DYNAMIC };
            }
        }
        /**
         * Checks whether a given query is used statically within the given class, its super
         * class or derived classes.
         */
        analyzeQueryUsage(classDecl, query, knownInputNames, functionCtx = new Map(), visitInheritedClasses = true) {
            const usageVisitor = new declaration_usage_visitor_1.DeclarationUsageVisitor(query.property, this.typeChecker, functionCtx);
            const classMetadata = this.classMetadata.get(classDecl);
            let usage = declaration_usage_visitor_1.ResolvedUsage.ASYNCHRONOUS;
            // In case there is metadata for the current class, we collect all resolved Angular input
            // names and add them to the list of known inputs that need to be checked for usages of
            // the current query. e.g. queries used in an @Input() *setter* are always static.
            if (classMetadata) {
                knownInputNames.push(...classMetadata.ngInputNames);
            }
            // Array of TypeScript nodes which can contain usages of the given query in
            // order to access it statically.
            const possibleStaticQueryNodes = filterQueryClassMemberNodes(classDecl, query, knownInputNames);
            // In case nodes that can possibly access a query statically have been found, check
            // if the query declaration is synchronously used within any of these nodes.
            if (possibleStaticQueryNodes.length) {
                possibleStaticQueryNodes.forEach(n => usage = combineResolvedUsage(usage, usageVisitor.getResolvedNodeUsage(n)));
            }
            if (!classMetadata) {
                return usage;
            }
            // In case there is a component template for the current class, we check if the
            // template statically accesses the current query. In case that's true, the query
            // can be marked as static.
            if (classMetadata.template && property_name_1.hasPropertyNameText(query.property.name)) {
                const template = classMetadata.template;
                const parsedHtml = parse_html_1.parseHtmlGracefully(template.content, template.filePath);
                const htmlVisitor = new template_usage_visitor_1.TemplateUsageVisitor(query.property.name.text);
                if (parsedHtml && htmlVisitor.isQueryUsedStatically(parsedHtml)) {
                    return declaration_usage_visitor_1.ResolvedUsage.SYNCHRONOUS;
                }
            }
            // In case derived classes should also be analyzed, we determine the classes that derive
            // from the current class and check if these have input setters or lifecycle hooks that
            // use the query statically.
            if (visitInheritedClasses) {
                classMetadata.derivedClasses.forEach(derivedClass => {
                    usage = combineResolvedUsage(usage, this.analyzeQueryUsage(derivedClass, query, knownInputNames));
                });
            }
            // In case the current class has a super class, we determine declared abstract function-like
            // declarations in the super-class that are implemented in the current class. The super class
            // will then be analyzed with the abstract declarations mapped to the implemented TypeScript
            // nodes. This allows us to handle queries which are used in super classes through derived
            // abstract method declarations.
            if (classMetadata.superClass) {
                const superClassDecl = classMetadata.superClass;
                // Update the function context to map abstract declaration nodes to their implementation
                // node in the base class. This ensures that the declaration usage visitor can analyze
                // abstract class member declarations.
                super_class_context_1.updateSuperClassAbstractMembersContext(classDecl, functionCtx, this.classMetadata);
                usage = combineResolvedUsage(usage, this.analyzeQueryUsage(superClassDecl, query, [], functionCtx, false));
            }
            return usage;
        }
    }
    exports.QueryUsageStrategy = QueryUsageStrategy;
    /**
     * Combines two resolved usages based on a fixed priority. "Synchronous" takes
     * precedence over "Ambiguous" whereas ambiguous takes precedence over "Asynchronous".
     */
    function combineResolvedUsage(base, target) {
        if (base === declaration_usage_visitor_1.ResolvedUsage.SYNCHRONOUS) {
            return base;
        }
        else if (target !== declaration_usage_visitor_1.ResolvedUsage.ASYNCHRONOUS) {
            return target;
        }
        else {
            return declaration_usage_visitor_1.ResolvedUsage.ASYNCHRONOUS;
        }
    }
    /**
     * Filters all class members from the class declaration that can access the
     * given query statically (e.g. ngOnInit lifecycle hook or @Input setters)
     */
    function filterQueryClassMemberNodes(classDecl, query, knownInputNames) {
        // Returns an array of TypeScript nodes which can contain usages of the given query
        // in order to access it statically. e.g.
        //  (1) queries used in the "ngOnInit" lifecycle hook are static.
        //  (2) inputs with setters can access queries statically.
        return classDecl.members
            .filter((m) => {
            if (ts.isMethodDeclaration(m) && m.body && property_name_1.hasPropertyNameText(m.name) &&
                STATIC_QUERY_LIFECYCLE_HOOKS[query.type].indexOf(m.name.text) !== -1) {
                return true;
            }
            else if (knownInputNames && ts.isSetAccessor(m) && m.body &&
                property_name_1.hasPropertyNameText(m.name) && knownInputNames.indexOf(m.name.text) !== -1) {
                return true;
            }
            return false;
        })
            .map(member => member.body);
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXNhZ2Vfc3RyYXRlZ3kuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NjaGVtYXRpY3MvbWlncmF0aW9ucy9zdGF0aWMtcXVlcmllcy9zdHJhdGVnaWVzL3VzYWdlX3N0cmF0ZWd5L3VzYWdlX3N0cmF0ZWd5LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgsaUNBQWlDO0lBRWpDLDBFQUFpRTtJQUNqRSwyRkFBK0U7SUFFL0Usa0hBQXlGO0lBR3pGLHNKQUFvRztJQUNwRywwSUFBNkU7SUFDN0UsZ0pBQThEO0lBRzlEOzs7T0FHRztJQUNILE1BQU0sNEJBQTRCLEdBQUc7UUFDbkMsQ0FBQyw0QkFBUyxDQUFDLFNBQVMsQ0FBQyxFQUNqQixDQUFDLGFBQWEsRUFBRSxVQUFVLEVBQUUsV0FBVyxFQUFFLG9CQUFvQixFQUFFLHVCQUF1QixDQUFDO1FBQzNGLENBQUMsNEJBQVMsQ0FBQyxZQUFZLENBQUMsRUFBRSxDQUFDLGFBQWEsRUFBRSxVQUFVLEVBQUUsV0FBVyxDQUFDO0tBQ25FLENBQUM7SUFFRjs7OztPQUlHO0lBQ0gsTUFBYSxrQkFBa0I7UUFDN0IsWUFBb0IsYUFBK0IsRUFBVSxXQUEyQjtZQUFwRSxrQkFBYSxHQUFiLGFBQWEsQ0FBa0I7WUFBVSxnQkFBVyxHQUFYLFdBQVcsQ0FBZ0I7UUFBRyxDQUFDO1FBRTVGLEtBQUssS0FBSSxDQUFDO1FBRVY7OztXQUdHO1FBQ0gsWUFBWSxDQUFDLEtBQXdCO1lBQ25DLElBQUksS0FBSyxDQUFDLFFBQVEsS0FBSyxJQUFJLEVBQUU7Z0JBQzNCLE9BQU8sRUFBQyxNQUFNLEVBQUUsSUFBSSxFQUFFLE9BQU8sRUFBRSxrREFBa0QsRUFBQyxDQUFDO2FBQ3BGO1lBRUQsTUFBTSxLQUFLLEdBQUcsSUFBSSxDQUFDLGlCQUFpQixDQUFDLEtBQUssQ0FBQyxTQUFTLEVBQUUs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QUM7Z0JBQ2xFLDRCQUE0QixDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUMsRUFBRTtnQkFDeEUsT0FBTyxJQUFJLENBQUM7YUFDYjtpQkFBTSxJQUNILGVBQWUsSUFBSSxFQUFFLENBQUMsYUFBYSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxJQUFJO2dCQUNoRCxtQ0FBbUIsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLElBQUksZUFBZSxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQyxFQUFFO2dCQUM5RSxPQUFPLElBQUksQ0FBQzthQUNiO1lBQ0QsT0FBTyxLQUFLLENBQUM7UUFDZixDQUFDLENBQUM7YUFDVCxHQUFHLENBQUMsTUFBTSxDQUFDLEVBQUUsQ0FBQyxNQUFNLENBQUMsSUFBTSxDQUFDLENBQUM7SUFDcEMsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0ICogYXMgdHMgZnJvbSAndHlwZXNjcmlwdCc7XG5cbmltcG9ydCB7cGFyc2VIdG1sR3JhY2VmdWxseX0gZnJvbSAnLi4vLi4vLi4vLi4vdXRpbHMvcGFyc2VfaHRtbCc7XG5pbXBvcnQge2hhc1Byb3BlcnR5TmFtZVRleHR9IGZyb20gJy4uLy4uLy4uLy4uL3V0aWxzL3R5cGVzY3JpcHQvcHJvcGVydHlfbmFtZSc7XG5pbXBvcnQge0NsYXNzTWV0YWRhdGFNYXB9IGZyb20gJy4uLy4uL2FuZ3VsYXIvbmdfcXVlcnlfdmlzaXRvcic7XG5pbXBvcnQge05nUXVlcnlEZWZpbml0aW9uLCBRdWVyeVRpbWluZywgUXVlcnlUeXBlfSBmcm9tICcuLi8uLi9hbmd1bGFyL3F1ZXJ5LWRlZmluaXRpb24nO1xuaW1wb3J0IHtUaW1pbmdSZXN1bHQsIFRpbWluZ1N0cmF0ZWd5fSBmcm9tICcuLi90aW1pbmctc3RyYXRlZ3knO1xuXG5pbXBvcnQge0RlY2xhcmF0aW9uVXNhZ2VWaXNpdG9yLCBGdW5jdGlvbkNvbnRleHQsIFJlc29sdmVkVXNhZ2V9IGZyb20gJy4vZGVjbGFyYXRpb25fdXNhZ2VfdmlzaXRvcic7XG5pbXBvcnQge3VwZGF0ZVN1cGVyQ2xhc3NBYnN0cmFjdE1lbWJlcnNDb250ZXh0fSBmcm9tICcuL3N1cGVyX2NsYXNzX2NvbnRleHQnO1xuaW1wb3J0IHtUZW1wbGF0ZVVzYWdlVmlzaXRvcn0gZnJvbSAnLi90ZW1wbGF0ZV91c2FnZV92aXNpdG9yJztcblxuXG4vKipcbiAqIE9iamVjdCB0aGF0IG1hcHMgYSBnaXZlbiB0eXBlIG9mIHF1ZXJ5IHRvIGEgbGlzdCBvZiBsaWZlY3ljbGUgaG9va3MgdGhhdFxuICogY291bGQgYmUgdXNlZCB0byBhY2Nlc3Mgc3VjaCBhIHF1ZXJ5IHN0YXRpY2FsbHkuXG4gKi9cbmNvbnN0IFNUQVRJQ19RVUVSWV9MSUZFQ1lDTEVfSE9PS1MgPSB7XG4gIFtRdWVyeVR5cGUuVmlld0NoaWxkXTpcbiAgICAgIFsnbmdPbkNoYW5nZXMnLCAnbmdPbkluaXQnLCAnbmdEb0NoZWNrJywgJ25nQWZ0ZXJDb250ZW50SW5pdCcsICduZ0FmdGVyQ29udGVudENoZWNrZWQnXSxcbiAgW1F1ZXJ5VHlwZS5Db250ZW50Q2hpbGRdOiBbJ25nT25DaGFuZ2VzJywgJ25nT25Jbml0JywgJ25nRG9DaGVjayddLFxufTtcblxuLyoqXG4gKiBRdWVyeSB0aW1pbmcgc3RyYXRlZ3kgdGhhdCBkZXRlcm1pbmVzIHRoZSB0aW1pbmcgb2YgYSBnaXZlbiBxdWVyeSBieSBpbnNwZWN0aW5nIGhvd1xuICogdGhlIHF1ZXJ5IGlzIGFjY2Vzc2VkIHdpdGhpbiB0aGUgcHJvamVjdCdzIFR5cGVTY3JpcHQgc291cmNlIGZpbGVzLiBSZWFkIG1vcmUgYWJvdXRcbiAqIHRoaXMgc3RyYXRlZ3kgaGVyZTogaHR0cHM6Ly9oYWNrbWQuaW8vcy9IeW12YzJPS0VcbiAqL1xuZXhwb3J0IGNsYXNzIFF1ZXJ5VXNhZ2VTdHJhdGVneSBpbXBsZW1lbnRzIFRpbWluZ1N0cmF0ZWd5IHtcbiAgY29uc3RydWN0b3IocHJpdmF0ZSBjbGFzc01ldGFkYXRhOiBDbGFzc01ldGFkYXRhTWFwLCBwcml2YXRlIHR5cGVDaGVja2VyOiB0cy5UeXBlQ2hlY2tlcikge31cblxuICBzZXR1cCgpIHt9XG5cbiAgLyoqXG4gICAqIEFuYWx5emVzIHRoZSB1c2FnZSBvZiB0aGUgZ2l2ZW4gcXVlcnkgYW5kIGRldGVybWluZXMgdGhlIHF1ZXJ5IHRpbWluZyBiYXNlZFxuICAgKiBvbiB0aGUgY3VycmVudCB1c2FnZSBvZiB0aGUgcXVlcnkuXG4gICAqL1xuICBkZXRlY3RUaW1pbmcocXVlcnk6IE5nUXVlcnlEZWZpbml0aW9uKTogVGltaW5nUmVzdWx0IHtcbiAgICBpZiAocXVlcnkucHJvcGVydHkgPT09IG51bGwpIHtcbiAgICAgIHJldHVybiB7dGltaW5nOiBudWxsLCBtZXNzYWdlOiAnUXVlcmllcyBkZWZpbmVkIG9uIGFjY2Vzc29ycyBjYW5ub3QgYmUgYW5hbHl6ZWQuJ307XG4gICAgfVxuXG4gICAgY29uc3QgdXNhZ2UgPSB0aGlzLmFuYWx5emVRdWVyeVVzYWdlKHF1ZXJ5LmNvbnRhaW5lciwgcXVlcnksIFtdKTtcblxuICAgIGlmICh1c2FnZSA9PT0gUmVzb2x2ZWRVc2FnZS5BTUJJR1VPVVMpIHtcbiAgICAgIHJldHVybiB7XG4gICAgICAgIHRpbWluZzogUXVlcnlUaW1pbmcuU1RBVElDLFxuICAgICAgICBtZXNzYWdlOiAnUXVlcnkgdGltaW5nIGlzIGFtYmlndW91cy4gUGxlYXNlIGNoZWNrIGlmIHRoZSBxdWVyeSBjYW4gYmUgbWFya2VkIGFzIGR5bmFtaWMuJ1xuICAgICAgfTtcbiAgICB9IGVsc2UgaWYgKHVzYWdlID09PSBSZXNvbHZlZFVzYWdlLlNZTkNIUk9OT1VTKSB7XG4gICAgICByZXR1cm4ge3RpbWluZzogUXVlcnlUaW1pbmcuU1RBVElDfTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHt0aW1pbmc6IFF1ZXJ5VGltaW5nLkRZTkFNSUN9O1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBDaGVja3Mgd2hldGhlciBhIGdpdmVuIHF1ZXJ5IGlzIHVzZWQgc3RhdGljYWxseSB3aXRoaW4gdGhlIGdpdmVuIGNsYXNzLCBpdHMgc3VwZXJcbiAgICogY2xhc3Mgb3IgZGVyaXZlZCBjbGFzc2VzLlxuICAgKi9cbiAgcHJpdmF0ZSBhbmFseXplUXVlcnlVc2FnZShcbiAgICAgIGNsYXNzRGVjbDogdHMuQ2xhc3NEZWNsYXJhdGlvbiwgcXVlcnk6IE5nUXVlcnlEZWZpbml0aW9uLCBrbm93bklucHV0TmFtZXM6IHN0cmluZ1tdLFxuICAgICAgZnVuY3Rpb25DdHg6IEZ1bmN0aW9uQ29udGV4dCA9IG5ldyBNYXAoKSwgdmlzaXRJbmhlcml0ZWRDbGFzc2VzID0gdHJ1ZSk6IFJlc29sdmVkVXNhZ2Uge1xuICAgIGNvbnN0IHVzYWdlVmlzaXRvciA9XG4gICAgICAgIG5ldyBEZWNsYXJhdGlvblVzYWdlVmlzaXRvcihxdWVyeS5wcm9wZXJ0eSAhLCB0aGlzLnR5cGVDaGVja2VyLCBmdW5jdGlvbkN0eCk7XG4gICAgY29uc3QgY2xhc3NNZXRhZGF0YSA9IHRoaXMuY2xhc3NNZXRhZGF0YS5nZXQoY2xhc3NEZWNsKTtcbiAgICBsZXQgdXNhZ2U6IFJlc29sdmVkVXNhZ2UgPSBSZXNvbHZlZFVzYWdlLkFTWU5DSFJPTk9VUztcblxuICAgIC8vIEluIGNhc2UgdGhlcmUgaXMgbWV0YWRhdGEgZm9yIHRoZSBjdXJyZW50IGNsYXNzLCB3ZSBjb2xsZWN0IGFsbCByZXNvbHZlZCBBbmd1bGFyIGlucHV0XG4gICAgLy8gbmFtZXMgYW5kIGFkZCB0aGVtIHRvIHRoZSBsaXN0IG9mIGtub3duIGlucHV0cyB0aGF0IG5lZWQgdG8gYmUgY2hlY2tlZCBmb3IgdXNhZ2VzIG9mXG4gICAgLy8gdGhlIGN1cnJlbnQgcXVlcnkuIGUuZy4gcXVlcmllcyB1c2VkIGluIGFuIEBJbnB1dCgpICpzZXR0ZXIqIGFyZSBhbHdheXMgc3RhdGljLlxuICAgIGlmIChjbGFzc01ldGFkYXRhKSB7XG4gICAgICBrbm93bklucHV0TmFtZXMucHVzaCguLi5jbGFzc01ldGFkYXRhLm5nSW5wdXROYW1lcyk7XG4gICAgfVxuXG4gICAgLy8gQXJyYXkgb2YgVHlwZVNjcmlwdCBub2RlcyB3aGljaCBjYW4gY29udGFpbiB1c2FnZXMgb2YgdGhlIGdpdmVuIHF1ZXJ5IGluXG4gICAgLy8gb3JkZXIgdG8gYWNjZXNzIGl0IHN0YXRpY2FsbHkuXG4gICAgY29uc3QgcG9zc2libGVTdGF0aWNRdWVyeU5vZGVzID0gZmlsdGVyUXVlcnlDbGFzc01lbWJlck5vZGVzKGNsYXNzRGVjbCwgcXVlcnksIGtub3duSW5wdXROYW1lcyk7XG5cbiAgICAvLyBJbiBjYXNlIG5vZGVzIHRoYXQgY2FuIHBvc3NpYmx5IGFjY2VzcyBhIHF1ZXJ5IHN0YXRpY2FsbHkgaGF2ZSBiZWVuIGZvdW5kLCBjaGVja1xuICAgIC8vIGlmIHRoZSBxdWVyeSBkZWNsYXJhdGlvbiBpcyBzeW5jaHJvbm91c2x5IHVzZWQgd2l0aGluIGFueSBvZiB0aGVzZSBub2Rlcy5cbiAgICBpZiAocG9zc2libGVTdGF0aWNRdWVyeU5vZGVzLmxlbmd0aCkge1xuICAgICAgcG9zc2libGVTdGF0aWNRdWVyeU5vZGVzLmZvckVhY2goXG4gICAgICAgICAgbiA9PiB1c2FnZSA9IGNvbWJpbmVSZXNvbHZlZFVzYWdlKHVzYWdlLCB1c2FnZVZpc2l0b3IuZ2V0UmVzb2x2ZWROb2RlVXNhZ2UobikpKTtcbiAgICB9XG5cbiAgICBpZiAoIWNsYXNzTWV0YWRhdGEpIHtcbiAgICAgIHJldHVybiB1c2FnZTtcbiAgICB9XG5cbiAgICAvLyBJbiBjYXNlIHRoZXJlIGlzIGEgY29tcG9uZW50IHRlbXBsYXRlIGZvciB0aGUgY3VycmVudCBjbGFzcywgd2UgY2hlY2sgaWYgdGhlXG4gICAgLy8gdGVtcGxhdGUgc3RhdGljYWxseSBhY2Nlc3NlcyB0aGUgY3VycmVudCBxdWVyeS4gSW4gY2FzZSB0aGF0J3MgdHJ1ZSwgdGhlIHF1ZXJ5XG4gICAgLy8gY2FuIGJlIG1hcmtlZCBhcyBzdGF0aWMuXG4gICAgaWYgKGNsYXNzTWV0YWRhdGEudGVtcGxhdGUgJiYgaGFzUHJvcGVydHlOYW1lVGV4dChxdWVyeS5wcm9wZXJ0eSAhLm5hbWUpKSB7XG4gICAgICBjb25zdCB0ZW1wbGF0ZSA9IGNsYXNzTWV0YWRhdGEudGVtcGxhdGU7XG4gICAgICBjb25zdCBwYXJzZWRIdG1sID0gcGFyc2VIdG1sR3JhY2VmdWxseSh0ZW1wbGF0ZS5jb250ZW50LCB0ZW1wbGF0ZS5maWxlUGF0aCk7XG4gICAgICBjb25zdCBodG1sVmlzaXRvciA9IG5ldyBUZW1wbGF0ZVVzYWdlVmlzaXRvcihxdWVyeS5wcm9wZXJ0eSAhLm5hbWUudGV4dCk7XG5cbiAgICAgIGlmIChwYXJzZWRIdG1sICYmIGh0bWxWaXNpdG9yLmlzUXVlcnlVc2VkU3RhdGljYWxseShwYXJzZWRIdG1sKSkge1xuICAgICAgICByZXR1cm4gUmVzb2x2ZWRVc2FnZS5TWU5DSFJPTk9VUztcbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBJbiBjYXNlIGRlcml2ZWQgY2xhc3NlcyBzaG91bGQgYWxzbyBiZSBhbmFseXplZCwgd2UgZGV0ZXJtaW5lIHRoZSBjbGFzc2VzIHRoYXQgZGVyaXZlXG4gICAgLy8gZnJvbSB0aGUgY3VycmVudCBjbGFzcyBhbmQgY2hlY2sgaWYgdGhlc2UgaGF2ZSBpbnB1dCBzZXR0ZXJzIG9yIGxpZmVjeWNsZSBob29rcyB0aGF0XG4gICAgLy8gdXNlIHRoZSBxdWVyeSBzdGF0aWNhbGx5LlxuICAgIGlmICh2aXNpdEluaGVyaXRlZENsYXNzZXMpIHtcbiAgICAgIGNsYXNzTWV0YWRhdGEuZGVyaXZlZENsYXNzZXMuZm9yRWFjaChkZXJpdmVkQ2xhc3MgPT4ge1xuICAgICAgICB1c2FnZSA9IGNvbWJpbmVSZXNvbHZlZFVzYWdlKFxuICAgICAgICAgICAgdXNhZ2UsIHRoaXMuYW5hbHl6ZVF1ZXJ5VXNhZ2UoZGVyaXZlZENsYXNzLCBxdWVyeSwga25vd25JbnB1dE5hbWVzKSk7XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICAvLyBJbiBjYXNlIHRoZSBjdXJyZW50IGNsYXNzIGhhcyBhIHN1cGVyIGNsYXNzLCB3ZSBkZXRlcm1pbmUgZGVjbGFyZWQgYWJzdHJhY3QgZnVuY3Rpb24tbGlrZVxuICAgIC8vIGRlY2xhcmF0aW9ucyBpbiB0aGUgc3VwZXItY2xhc3MgdGhhdCBhcmUgaW1wbGVtZW50ZWQgaW4gdGhlIGN1cnJlbnQgY2xhc3MuIFRoZSBzdXBlciBjbGFzc1xuICAgIC8vIHdpbGwgdGhlbiBiZSBhbmFseXplZCB3aXRoIHRoZSBhYnN0cmFjdCBkZWNsYXJhdGlvbnMgbWFwcGVkIHRvIHRoZSBpbXBsZW1lbnRlZCBUeXBlU2NyaXB0XG4gICAgLy8gbm9kZXMuIFRoaXMgYWxsb3dzIHVzIHRvIGhhbmRsZSBxdWVyaWVzIHdoaWNoIGFyZSB1c2VkIGluIHN1cGVyIGNsYXNzZXMgdGhyb3VnaCBkZXJpdmVkXG4gICAgLy8gYWJzdHJhY3QgbWV0aG9kIGRlY2xhcmF0aW9ucy5cbiAgICBpZiAoY2xhc3NNZXRhZGF0YS5zdXBlckNsYXNzKSB7XG4gICAgICBjb25zdCBzdXBlckNsYXNzRGVjbCA9IGNsYXNzTWV0YWRhdGEuc3VwZXJDbGFzcztcblxuICAgICAgLy8gVXBkYXRlIHRoZSBmdW5jdGlvbiBjb250ZXh0IHRvIG1hcCBhYnN0cmFjdCBkZWNsYXJhdGlvbiBub2RlcyB0byB0aGVpciBpbXBsZW1lbnRhdGlvblxuICAgICAgLy8gbm9kZSBpbiB0aGUgYmFzZSBjbGFzcy4gVGhpcyBlbnN1cmVzIHRoYXQgdGhlIGRlY2xhcmF0aW9uIHVzYWdlIHZpc2l0b3IgY2FuIGFuYWx5emVcbiAgICAgIC8vIGFic3RyYWN0IGNsYXNzIG1lbWJlciBkZWNsYXJhdGlvbnMuXG4gICAgICB1cGRhdGVTdXBlckNsYXNzQWJzdHJhY3RNZW1iZXJzQ29udGV4dChjbGFzc0RlY2wsIGZ1bmN0aW9uQ3R4LCB0aGlzLmNsYXNzTWV0YWRhdGEpO1xuXG4gICAgICB1c2FnZSA9IGNvbWJpbmVSZXNvbHZlZFVzYWdlKFxuICAgICAgICAgIHVzYWdlLCB0aGlzLmFuYWx5emVRdWVyeVVzYWdlKHN1cGVyQ2xhc3NEZWNsLCBxdWVyeSwgW10sIGZ1bmN0aW9uQ3R4LCBmYWxzZSkpO1xuICAgIH1cblxuICAgIHJldHVybiB1c2FnZTtcbiAgfVxufVxuXG4vKipcbiAqIENvbWJpbmVzIHR3byByZXNvbHZlZCB1c2FnZXMgYmFzZWQgb24gYSBmaXhlZCBwcmlvcml0eS4gXCJTeW5jaHJvbm91c1wiIHRha2VzXG4gKiBwcmVjZWRlbmNlIG92ZXIgXCJBbWJpZ3VvdXNcIiB3aGVyZWFzIGFtYmlndW91cyB0YWtlcyBwcmVjZWRlbmNlIG92ZXIgXCJBc3luY2hyb25vdXNcIi5cbiAqL1xuZnVuY3Rpb24gY29tYmluZVJlc29sdmVkVXNhZ2UoYmFzZTogUmVzb2x2ZWRVc2FnZSwgdGFyZ2V0OiBSZXNvbHZlZFVzYWdlKTogUmVzb2x2ZWRVc2FnZSB7XG4gIGlmIChiYXNlID09PSBSZXNvbHZlZFVzYWdlLlNZTkNIUk9OT1VTKSB7XG4gICAgcmV0dXJuIGJhc2U7XG4gIH0gZWxzZSBpZiAodGFyZ2V0ICE9PSBSZXNvbHZlZFVzYWdlLkFTWU5DSFJPTk9VUykge1xuICAgIHJldHVybiB0YXJnZXQ7XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIFJlc29sdmVkVXNhZ2UuQVNZTkNIUk9OT1VTO1xuICB9XG59XG5cbi8qKlxuICogRmlsdGVycyBhbGwgY2xhc3MgbWVtYmVycyBmcm9tIHRoZSBjbGFzcyBkZWNsYXJhdGlvbiB0aGF0IGNhbiBhY2Nlc3MgdGhlXG4gKiBnaXZlbiBxdWVyeSBzdGF0aWNhbGx5IChlLmcuIG5nT25Jbml0IGxpZmVjeWNsZSBob29rIG9yIEBJbnB1dCBzZXR0ZXJzKVxuICovXG5mdW5jdGlvbiBmaWx0ZXJRdWVyeUNsYXNzTWVtYmVyTm9kZXMoXG4gICAgY2xhc3NEZWNsOiB0cy5DbGFzc0RlY2xhcmF0aW9uLCBxdWVyeTogTmdRdWVyeURlZmluaXRpb24sXG4gICAga25vd25JbnB1dE5hbWVzOiBzdHJpbmdbXSk6IHRzLkJsb2NrW10ge1xuICAvLyBSZXR1cm5zIGFuIGFycmF5IG9mIFR5cGVTY3JpcHQgbm9kZXMgd2hpY2ggY2FuIGNvbnRhaW4gdXNhZ2VzIG9mIHRoZSBnaXZlbiBxdWVyeVxuICAvLyBpbiBvcmRlciB0byBhY2Nlc3MgaXQgc3RhdGljYWxseS4gZS5nLlxuICAvLyAgKDEpIHF1ZXJpZXMgdXNlZCBpbiB0aGUgXCJuZ09uSW5pdFwiIGxpZmVjeWNsZSBob29rIGFyZSBzdGF0aWMuXG4gIC8vICAoMikgaW5wdXRzIHdpdGggc2V0dGVycyBjYW4gYWNjZXNzIHF1ZXJpZXMgc3RhdGljYWxseS5cbiAgcmV0dXJuIGNsYXNzRGVjbC5tZW1iZXJzXG4gICAgICAuZmlsdGVyKFxuICAgICAgICAgIChtKTpcbiAgICAgICAgICAgICAgbSBpcyh0cy5TZXRBY2Nlc3NvckRlY2xhcmF0aW9uIHwgdHMuTWV0aG9kRGVjbGFyYXRpb24pID0+IHtcbiAgICAgICAgICAgICAgICBpZiAodHMuaXNNZXRob2REZWNsYXJhdGlvbihtKSAmJiBtLmJvZHkgJiYgaGFzUHJvcGVydHlOYW1lVGV4dChtLm5hbWUpICYmXG4gICAgICAgICAgICAgICAgICAgIFNUQVRJQ19RVUVSWV9MSUZFQ1lDTEVfSE9PS1NbcXVlcnkudHlwZV0uaW5kZXhPZihtLm5hbWUudGV4dCkgIT09IC0xKSB7XG4gICAgICAgICAgICAgICAgICByZXR1cm4gdHJ1ZTtcbiAgICAgICAgICAgICAgICB9IGVsc2UgaWYgKFxuICAgICAgICAgICAgICAgICAgICBrbm93bklucHV0TmFtZXMgJiYgdHMuaXNTZXRBY2Nlc3NvcihtKSAmJiBtLmJvZHkgJiZcbiAgICAgICAgICAgICAgICAgICAgaGFzUHJvcGVydHlOYW1lVGV4dChtLm5hbWUpICYmIGtub3duSW5wdXROYW1lcy5pbmRleE9mKG0ubmFtZS50ZXh0KSAhPT0gLTEpIHtcbiAgICAgICAgICAgICAgICAgIHJldHVybiB0cnVlO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICByZXR1cm4gZmFsc2U7XG4gICAgICAgICAgICAgIH0pXG4gICAgICAubWFwKG1lbWJlciA9PiBtZW1iZXIuYm9keSAhKTtcbn1cbiJdfQ==