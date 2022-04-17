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
        define("@angular/core/schematics/migrations/static-queries/strategies/usage_strategy/template_usage_visitor", ["require", "exports", "@angular/compiler", "@angular/compiler/src/render3/r3_ast"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const compiler_1 = require("@angular/compiler");
    const r3_ast_1 = require("@angular/compiler/src/render3/r3_ast");
    /**
     * AST visitor that traverses the Render3 HTML AST in order to check if the given
     * query property is accessed statically in the template.
     */
    class TemplateUsageVisitor extends r3_ast_1.NullVisitor {
        constructor(queryPropertyName) {
            super();
            this.queryPropertyName = queryPropertyName;
            this.hasQueryTemplateReference = false;
            this.expressionAstVisitor = new ExpressionAstVisitor(this.queryPropertyName);
        }
        /** Checks whether the given query is statically accessed within the specified HTML nodes. */
        isQueryUsedStatically(htmlNodes) {
            this.hasQueryTemplateReference = false;
            this.expressionAstVisitor.hasQueryPropertyRead = false;
            // Visit all AST nodes and check if the query property is used statically.
            r3_ast_1.visitAll(this, htmlNodes);
            return !this.hasQueryTemplateReference && this.expressionAstVisitor.hasQueryPropertyRead;
        }
        visitElement(element) {
            // In case there is a template references variable that matches the query property
            // name, we can finish this visitor as such a template variable can be used in the
            // entire template and the query therefore can't be accessed from the template.
            if (element.references.some(r => r.name === this.queryPropertyName)) {
                this.hasQueryTemplateReference = true;
                return;
            }
            r3_ast_1.visitAll(this, element.attributes);
            r3_ast_1.visitAll(this, element.inputs);
            r3_ast_1.visitAll(this, element.outputs);
            r3_ast_1.visitAll(this, element.children);
        }
        visitTemplate(template) {
            r3_ast_1.visitAll(this, template.attributes);
            r3_ast_1.visitAll(this, template.inputs);
            r3_ast_1.visitAll(this, template.outputs);
            // We don't want to visit any children of the template as these never can't
            // access a query statically. The templates can be rendered in the ngAfterViewInit"
            // lifecycle hook at the earliest.
        }
        visitBoundAttribute(attribute) {
            attribute.value.visit(this.expressionAstVisitor, attribute.sourceSpan);
        }
        visitBoundText(text) { text.value.visit(this.expressionAstVisitor, text.sourceSpan); }
        visitBoundEvent(node) {
            node.handler.visit(this.expressionAstVisitor, node.handlerSpan);
        }
    }
    exports.TemplateUsageVisitor = TemplateUsageVisitor;
    /**
     * AST visitor that checks if the given expression contains property reads that
     * refer to the specified query property name.
     */
    class ExpressionAstVisitor extends compiler_1.RecursiveAstVisitor {
        constructor(queryPropertyName) {
            super();
            this.queryPropertyName = queryPropertyName;
            this.hasQueryPropertyRead = false;
        }
        visitPropertyRead(node, span) {
            // The receiver of the property read needs to be "implicit" as queries are accessed
            // from the component instance and not from other objects.
            if (node.receiver instanceof compiler_1.ImplicitReceiver && node.name === this.queryPropertyName) {
                this.hasQueryPropertyRead = true;
                return;
            }
            super.visitPropertyRead(node, span);
        }
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGVtcGxhdGVfdXNhZ2VfdmlzaXRvci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc2NoZW1hdGljcy9taWdyYXRpb25zL3N0YXRpYy1xdWVyaWVzL3N0cmF0ZWdpZXMvdXNhZ2Vfc3RyYXRlZ3kvdGVtcGxhdGVfdXNhZ2VfdmlzaXRvci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILGdEQUF1RztJQUN2RyxpRUFBMkk7SUFFM0k7OztPQUdHO0lBQ0gsTUFBYSxvQkFBcUIsU0FBUSxvQkFBVztRQUluRCxZQUFtQixpQkFBeUI7WUFBSSxLQUFLLEVBQUUsQ0FBQztZQUFyQyxzQkFBaUIsR0FBakIsaUJBQWlCLENBQVE7WUFIcEMsOEJBQXlCLEdBQUcsS0FBSyxDQUFDO1lBQ2xDLHlCQUFvQixHQUFHLElBQUksb0JBQW9CLENBQUMsSUFBSSxDQUFDLGlCQUFpQixDQUFDLENBQUM7UUFFdkIsQ0FBQztRQUUxRCw2RkFBNkY7UUFDN0YscUJBQXFCLENBQUMsU0FBaUI7WUFDckMsSUFBSSxDQUFDLHlCQUF5QixHQUFHLEtBQUssQ0FBQztZQUN2QyxJQUFJLENBQUMsb0JBQW9CLENBQUMsb0JBQW9CLEdBQUcsS0FBSyxDQUFDO1lBRXZELDBFQUEwRTtZQUMxRSxpQkFBUSxDQUFDLElBQUksRUFBRSxTQUFTLENBQUMsQ0FBQztZQUUxQixPQUFPLENBQUMsSUFBSSxDQUFDLHlCQUF5QixJQUFJLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxvQkFBb0IsQ0FBQztRQUMzRixDQUFDO1FBRUQsWUFBWSxDQUFDLE9BQWdCO1lBQzNCLGtGQUFrRjtZQUNsRixrRkFBa0Y7WUFDbEYsK0VBQStFO1lBQy9FLElBQUksT0FBTyxDQUFDLFVBQVUsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsSUFBSSxLQUFLLElBQUksQ0FBQyxpQkFBaUIsQ0FBQyxFQUFFO2dCQUNuRSxJQUFJLENBQUMseUJBQXlCLEdBQUcsSUFBSSxDQUFDO2dCQUN0QyxPQUFPO2FBQ1I7WUFFRCxpQkFBUSxDQUFDLElBQUksRUFBRSxPQUFPLENBQUMsVUFBVSxDQUFDLENBQUM7WUFDbkMsaUJBQVEsQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDLE1BQU0sQ0FBQyxDQUFDO1lBQy9CLGlCQUFRLENBQUMsSUFBSSxFQUFFLE9BQU8sQ0FBQyxPQUFPLENBQUMsQ0FBQztZQUNoQyxpQkFBUSxDQUFDLElBQUksRUFBRSxPQUFPLENBQUMsUUFBUSxDQUFDLENBQUM7UUFDbkMsQ0FBQztRQUVELGFBQWEsQ0FBQyxRQUFrQjtZQUM5QixpQkFBUSxDQUFDLElBQUksRUFBRSxRQUFRLENBQUMsVUFBVSxDQUFDLENBQUM7WUFDcEMsaUJBQVEsQ0FBQyxJQUFJLEVBQUUsUUFBUSxDQUFDLE1BQU0sQ0FBQyxDQUFDO1lBQ2hDLGlCQUFRLENBQUMsSUFBSSxFQUFFLFFBQVEsQ0FBQyxPQUFPLENBQUMsQ0FBQztZQUVqQywyRUFBMkU7WUFDM0UsbUZBQW1GO1lBQ25GLGtDQUFrQztRQUNwQyxDQUFDO1FBRUQsbUJBQW1CLENBQUMsU0FBeUI7WUFDM0MsU0FBUyxDQUFDLEtBQUssQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLG9CQUFvQixFQUFFLFNBQVMsQ0FBQyxVQUFVLENBQUMsQ0FBQztRQUN6RSxDQUFDO1FBRUQsY0FBYyxDQUFDLElBQWUsSUFBSSxJQUFJLENBQUMsS0FBSyxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsb0JBQW9CLEVBQUUsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUVqRyxlQUFlLENBQUMsSUFBZ0I7WUFDOUIsSUFBSSxDQUFDLE9BQU8sQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLG9CQUFvQixFQUFFLElBQUksQ0FBQyxXQUFXLENBQUMsQ0FBQztRQUNsRSxDQUFDO0tBQ0Y7SUFuREQsb0RBbURDO0lBRUQ7OztPQUdHO0lBQ0gsTUFBTSxvQkFBcUIsU0FBUSw4QkFBbUI7UUFHcEQsWUFBb0IsaUJBQXlCO1lBQUksS0FBSyxFQUFFLENBQUM7WUFBckMsc0JBQWlCLEdBQWpCLGlCQUFpQixDQUFRO1lBRjdDLHlCQUFvQixHQUFHLEtBQUssQ0FBQztRQUU2QixDQUFDO1FBRTNELGlCQUFpQixDQUFDLElBQWtCLEVBQUUsSUFBcUI7WUFDekQsbUZBQW1GO1lBQ25GLDBEQUEwRDtZQUMxRCxJQUFJLElBQUksQ0FBQyxRQUFRLFlBQVksMkJBQWdCLElBQUksSUFBSSxDQUFDLElBQUksS0FBSyxJQUFJLENBQUMsaUJBQWlCLEVBQUU7Z0JBQ3JGLElBQUksQ0FBQyxvQkFBb0IsR0FBRyxJQUFJLENBQUM7Z0JBQ2pDLE9BQU87YUFDUjtZQUVELEtBQUssQ0FBQyxpQkFBaUIsQ0FBQyxJQUFJLEVBQUUsSUFBSSxDQUFDLENBQUM7UUFDdEMsQ0FBQztLQUNGIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0ltcGxpY2l0UmVjZWl2ZXIsIFBhcnNlU291cmNlU3BhbiwgUHJvcGVydHlSZWFkLCBSZWN1cnNpdmVBc3RWaXNpdG9yfSBmcm9tICdAYW5ndWxhci9jb21waWxlcic7XG5pbXBvcnQge0JvdW5kQXR0cmlidXRlLCBCb3VuZEV2ZW50LCBCb3VuZFRleHQsIEVsZW1lbnQsIE5vZGUsIE51bGxWaXNpdG9yLCBUZW1wbGF0ZSwgdmlzaXRBbGx9IGZyb20gJ0Bhbmd1bGFyL2NvbXBpbGVyL3NyYy9yZW5kZXIzL3IzX2FzdCc7XG5cbi8qKlxuICogQVNUIHZpc2l0b3IgdGhhdCB0cmF2ZXJzZXMgdGhlIFJlbmRlcjMgSFRNTCBBU1QgaW4gb3JkZXIgdG8gY2hlY2sgaWYgdGhlIGdpdmVuXG4gKiBxdWVyeSBwcm9wZXJ0eSBpcyBhY2Nlc3NlZCBzdGF0aWNhbGx5IGluIHRoZSB0ZW1wbGF0ZS5cbiAqL1xuZXhwb3J0IGNsYXNzIFRlbXBsYXRlVXNhZ2VWaXNpdG9yIGV4dGVuZHMgTnVsbFZpc2l0b3Ige1xuICBwcml2YXRlIGhhc1F1ZXJ5VGVtcGxhdGVSZWZlcmVuY2UgPSBmYWxzZTtcbiAgcHJpdmF0ZSBleHByZXNzaW9uQXN0VmlzaXRvciA9IG5ldyBFeHByZXNzaW9uQXN0VmlzaXRvcih0aGlzLnF1ZXJ5UHJvcGVydHlOYW1lKTtcblxuICBjb25zdHJ1Y3RvcihwdWJsaWMgcXVlcnlQcm9wZXJ0eU5hbWU6IHN0cmluZykgeyBzdXBlcigpOyB9XG5cbiAgLyoqIENoZWNrcyB3aGV0aGVyIHRoZSBnaXZlbiBxdWVyeSBpcyBzdGF0aWNhbGx5IGFjY2Vzc2VkIHdpdGhpbiB0aGUgc3BlY2lmaWVkIEhUTUwgbm9kZXMuICovXG4gIGlzUXVlcnlVc2VkU3RhdGljYWxseShodG1sTm9kZXM6IE5vZGVbXSk6IGJvb2xlYW4ge1xuICAgIHRoaXMuaGFzUXVlcnlUZW1wbGF0ZVJlZmVyZW5jZSA9IGZhbHNlO1xuICAgIHRoaXMuZXhwcmVzc2lvbkFzdFZpc2l0b3IuaGFzUXVlcnlQcm9wZXJ0eVJlYWQgPSBmYWxzZTtcblxuICAgIC8vIFZpc2l0IGFsbCBBU1Qgbm9kZXMgYW5kIGNoZWNrIGlmIHRoZSBxdWVyeSBwcm9wZXJ0eSBpcyB1c2VkIHN0YXRpY2FsbHkuXG4gICAgdmlzaXRBbGwodGhpcywgaHRtbE5vZGVzKTtcblxuICAgIHJldHVybiAhdGhpcy5oYXNRdWVyeVRlbXBsYXRlUmVmZXJlbmNlICYmIHRoaXMuZXhwcmVzc2lvbkFzdFZpc2l0b3IuaGFzUXVlcnlQcm9wZXJ0eVJlYWQ7XG4gIH1cblxuICB2aXNpdEVsZW1lbnQoZWxlbWVudDogRWxlbWVudCk6IHZvaWQge1xuICAgIC8vIEluIGNhc2UgdGhlcmUgaXMgYSB0ZW1wbGF0ZSByZWZlcmVuY2VzIHZhcmlhYmxlIHRoYXQgbWF0Y2hlcyB0aGUgcXVlcnkgcHJvcGVydHlcbiAgICAvLyBuYW1lLCB3ZSBjYW4gZmluaXNoIHRoaXMgdmlzaXRvciBhcyBzdWNoIGEgdGVtcGxhdGUgdmFyaWFibGUgY2FuIGJlIHVzZWQgaW4gdGhlXG4gICAgLy8gZW50aXJlIHRlbXBsYXRlIGFuZCB0aGUgcXVlcnkgdGhlcmVmb3JlIGNhbid0IGJlIGFjY2Vzc2VkIGZyb20gdGhlIHRlbXBsYXRlLlxuICAgIGlmIChlbGVtZW50LnJlZmVyZW5jZXMuc29tZShyID0+IHIubmFtZSA9PT0gdGhpcy5xdWVyeVByb3BlcnR5TmFtZSkpIHtcbiAgICAgIHRoaXMuaGFzUXVlcnlUZW1wbGF0ZVJlZmVyZW5jZSA9IHRydWU7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgdmlzaXRBbGwodGhpcywgZWxlbWVudC5hdHRyaWJ1dGVzKTtcbiAgICB2aXNpdEFsbCh0aGlzLCBlbGVtZW50LmlucHV0cyk7XG4gICAgdmlzaXRBbGwodGhpcywgZWxlbWVudC5vdXRwdXRzKTtcbiAgICB2aXNpdEFsbCh0aGlzLCBlbGVtZW50LmNoaWxkcmVuKTtcbiAgfVxuXG4gIHZpc2l0VGVtcGxhdGUodGVtcGxhdGU6IFRlbXBsYXRlKTogdm9pZCB7XG4gICAgdmlzaXRBbGwodGhpcywgdGVtcGxhdGUuYXR0cmlidXRlcyk7XG4gICAgdmlzaXRBbGwodGhpcywgdGVtcGxhdGUuaW5wdXRzKTtcbiAgICB2aXNpdEFsbCh0aGlzLCB0ZW1wbGF0ZS5vdXRwdXRzKTtcblxuICAgIC8vIFdlIGRvbid0IHdhbnQgdG8gdmlzaXQgYW55IGNoaWxkcmVuIG9mIHRoZSB0ZW1wbGF0ZSBhcyB0aGVzZSBuZXZlciBjYW4ndFxuICAgIC8vIGFjY2VzcyBhIHF1ZXJ5IHN0YXRpY2FsbHkuIFRoZSB0ZW1wbGF0ZXMgY2FuIGJlIHJlbmRlcmVkIGluIHRoZSBuZ0FmdGVyVmlld0luaXRcIlxuICAgIC8vIGxpZmVjeWNsZSBob29rIGF0IHRoZSBlYXJsaWVzdC5cbiAgfVxuXG4gIHZpc2l0Qm91bmRBdHRyaWJ1dGUoYXR0cmlidXRlOiBCb3VuZEF0dHJpYnV0ZSkge1xuICAgIGF0dHJpYnV0ZS52YWx1ZS52aXNpdCh0aGlzLmV4cHJlc3Npb25Bc3RWaXNpdG9yLCBhdHRyaWJ1dGUuc291cmNlU3Bhbik7XG4gIH1cblxuICB2aXNpdEJvdW5kVGV4dCh0ZXh0OiBCb3VuZFRleHQpIHsgdGV4dC52YWx1ZS52aXNpdCh0aGlzLmV4cHJlc3Npb25Bc3RWaXNpdG9yLCB0ZXh0LnNvdXJjZVNwYW4pOyB9XG5cbiAgdmlzaXRCb3VuZEV2ZW50KG5vZGU6IEJvdW5kRXZlbnQpIHtcbiAgICBub2RlLmhhbmRsZXIudmlzaXQodGhpcy5leHByZXNzaW9uQXN0VmlzaXRvciwgbm9kZS5oYW5kbGVyU3Bhbik7XG4gIH1cbn1cblxuLyoqXG4gKiBBU1QgdmlzaXRvciB0aGF0IGNoZWNrcyBpZiB0aGUgZ2l2ZW4gZXhwcmVzc2lvbiBjb250YWlucyBwcm9wZXJ0eSByZWFkcyB0aGF0XG4gKiByZWZlciB0byB0aGUgc3BlY2lmaWVkIHF1ZXJ5IHByb3BlcnR5IG5hbWUuXG4gKi9cbmNsYXNzIEV4cHJlc3Npb25Bc3RWaXNpdG9yIGV4dGVuZHMgUmVjdXJzaXZlQXN0VmlzaXRvciB7XG4gIGhhc1F1ZXJ5UHJvcGVydHlSZWFkID0gZmFsc2U7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBxdWVyeVByb3BlcnR5TmFtZTogc3RyaW5nKSB7IHN1cGVyKCk7IH1cblxuICB2aXNpdFByb3BlcnR5UmVhZChub2RlOiBQcm9wZXJ0eVJlYWQsIHNwYW46IFBhcnNlU291cmNlU3Bhbik6IGFueSB7XG4gICAgLy8gVGhlIHJlY2VpdmVyIG9mIHRoZSBwcm9wZXJ0eSByZWFkIG5lZWRzIHRvIGJlIFwiaW1wbGljaXRcIiBhcyBxdWVyaWVzIGFyZSBhY2Nlc3NlZFxuICAgIC8vIGZyb20gdGhlIGNvbXBvbmVudCBpbnN0YW5jZSBhbmQgbm90IGZyb20gb3RoZXIgb2JqZWN0cy5cbiAgICBpZiAobm9kZS5yZWNlaXZlciBpbnN0YW5jZW9mIEltcGxpY2l0UmVjZWl2ZXIgJiYgbm9kZS5uYW1lID09PSB0aGlzLnF1ZXJ5UHJvcGVydHlOYW1lKSB7XG4gICAgICB0aGlzLmhhc1F1ZXJ5UHJvcGVydHlSZWFkID0gdHJ1ZTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBzdXBlci52aXNpdFByb3BlcnR5UmVhZChub2RlLCBzcGFuKTtcbiAgfVxufVxuIl19