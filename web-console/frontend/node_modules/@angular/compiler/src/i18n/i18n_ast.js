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
        define("@angular/compiler/src/i18n/i18n_ast", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var Message = /** @class */ (function () {
        /**
         * @param nodes message AST
         * @param placeholders maps placeholder names to static content
         * @param placeholderToMessage maps placeholder names to messages (used for nested ICU messages)
         * @param meaning
         * @param description
         * @param id
         */
        function Message(nodes, placeholders, placeholderToMessage, meaning, description, id) {
            this.nodes = nodes;
            this.placeholders = placeholders;
            this.placeholderToMessage = placeholderToMessage;
            this.meaning = meaning;
            this.description = description;
            this.id = id;
            if (nodes.length) {
                this.sources = [{
                        filePath: nodes[0].sourceSpan.start.file.url,
                        startLine: nodes[0].sourceSpan.start.line + 1,
                        startCol: nodes[0].sourceSpan.start.col + 1,
                        endLine: nodes[nodes.length - 1].sourceSpan.end.line + 1,
                        endCol: nodes[0].sourceSpan.start.col + 1
                    }];
            }
            else {
                this.sources = [];
            }
        }
        return Message;
    }());
    exports.Message = Message;
    var Text = /** @class */ (function () {
        function Text(value, sourceSpan) {
            this.value = value;
            this.sourceSpan = sourceSpan;
        }
        Text.prototype.visit = function (visitor, context) { return visitor.visitText(this, context); };
        return Text;
    }());
    exports.Text = Text;
    // TODO(vicb): do we really need this node (vs an array) ?
    var Container = /** @class */ (function () {
        function Container(children, sourceSpan) {
            this.children = children;
            this.sourceSpan = sourceSpan;
        }
        Container.prototype.visit = function (visitor, context) { return visitor.visitContainer(this, context); };
        return Container;
    }());
    exports.Container = Container;
    var Icu = /** @class */ (function () {
        function Icu(expression, type, cases, sourceSpan) {
            this.expression = expression;
            this.type = type;
            this.cases = cases;
            this.sourceSpan = sourceSpan;
        }
        Icu.prototype.visit = function (visitor, context) { return visitor.visitIcu(this, context); };
        return Icu;
    }());
    exports.Icu = Icu;
    var TagPlaceholder = /** @class */ (function () {
        function TagPlaceholder(tag, attrs, startName, closeName, children, isVoid, sourceSpan) {
            this.tag = tag;
            this.attrs = attrs;
            this.startName = startName;
            this.closeName = closeName;
            this.children = children;
            this.isVoid = isVoid;
            this.sourceSpan = sourceSpan;
        }
        TagPlaceholder.prototype.visit = function (visitor, context) { return visitor.visitTagPlaceholder(this, context); };
        return TagPlaceholder;
    }());
    exports.TagPlaceholder = TagPlaceholder;
    var Placeholder = /** @class */ (function () {
        function Placeholder(value, name, sourceSpan) {
            this.value = value;
            this.name = name;
            this.sourceSpan = sourceSpan;
        }
        Placeholder.prototype.visit = function (visitor, context) { return visitor.visitPlaceholder(this, context); };
        return Placeholder;
    }());
    exports.Placeholder = Placeholder;
    var IcuPlaceholder = /** @class */ (function () {
        function IcuPlaceholder(value, name, sourceSpan) {
            this.value = value;
            this.name = name;
            this.sourceSpan = sourceSpan;
        }
        IcuPlaceholder.prototype.visit = function (visitor, context) { return visitor.visitIcuPlaceholder(this, context); };
        return IcuPlaceholder;
    }());
    exports.IcuPlaceholder = IcuPlaceholder;
    // Clone the AST
    var CloneVisitor = /** @class */ (function () {
        function CloneVisitor() {
        }
        CloneVisitor.prototype.visitText = function (text, context) { return new Text(text.value, text.sourceSpan); };
        CloneVisitor.prototype.visitContainer = function (container, context) {
            var _this = this;
            var children = container.children.map(function (n) { return n.visit(_this, context); });
            return new Container(children, container.sourceSpan);
        };
        CloneVisitor.prototype.visitIcu = function (icu, context) {
            var _this = this;
            var cases = {};
            Object.keys(icu.cases).forEach(function (key) { return cases[key] = icu.cases[key].visit(_this, context); });
            var msg = new Icu(icu.expression, icu.type, cases, icu.sourceSpan);
            msg.expressionPlaceholder = icu.expressionPlaceholder;
            return msg;
        };
        CloneVisitor.prototype.visitTagPlaceholder = function (ph, context) {
            var _this = this;
            var children = ph.children.map(function (n) { return n.visit(_this, context); });
            return new TagPlaceholder(ph.tag, ph.attrs, ph.startName, ph.closeName, children, ph.isVoid, ph.sourceSpan);
        };
        CloneVisitor.prototype.visitPlaceholder = function (ph, context) {
            return new Placeholder(ph.value, ph.name, ph.sourceSpan);
        };
        CloneVisitor.prototype.visitIcuPlaceholder = function (ph, context) {
            return new IcuPlaceholder(ph.value, ph.name, ph.sourceSpan);
        };
        return CloneVisitor;
    }());
    exports.CloneVisitor = CloneVisitor;
    // Visit all the nodes recursively
    var RecurseVisitor = /** @class */ (function () {
        function RecurseVisitor() {
        }
        RecurseVisitor.prototype.visitText = function (text, context) { };
        RecurseVisitor.prototype.visitContainer = function (container, context) {
            var _this = this;
            container.children.forEach(function (child) { return child.visit(_this); });
        };
        RecurseVisitor.prototype.visitIcu = function (icu, context) {
            var _this = this;
            Object.keys(icu.cases).forEach(function (k) { icu.cases[k].visit(_this); });
        };
        RecurseVisitor.prototype.visitTagPlaceholder = function (ph, context) {
            var _this = this;
            ph.children.forEach(function (child) { return child.visit(_this); });
        };
        RecurseVisitor.prototype.visitPlaceholder = function (ph, context) { };
        RecurseVisitor.prototype.visitIcuPlaceholder = function (ph, context) { };
        return RecurseVisitor;
    }());
    exports.RecurseVisitor = RecurseVisitor;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaTE4bl9hc3QuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvaTE4bi9pMThuX2FzdC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUlIO1FBR0U7Ozs7Ozs7V0FPRztRQUNILGlCQUNXLEtBQWEsRUFBUyxZQUF3QyxFQUM5RCxvQkFBaUQsRUFBUyxPQUFlLEVBQ3pFLFdBQW1CLEVBQVMsRUFBVTtZQUZ0QyxVQUFLLEdBQUwsS0FBSyxDQUFRO1lBQVMsaUJBQVksR0FBWixZQUFZLENBQTRCO1lBQzlELHlCQUFvQixHQUFwQixvQkFBb0IsQ0FBNkI7WUFBUyxZQUFPLEdBQVAsT0FBTyxDQUFRO1lBQ3pFLGdCQUFXLEdBQVgsV0FBVyxDQUFRO1lBQVMsT0FBRSxHQUFGLEVBQUUsQ0FBUTtZQUMvQyxJQUFJLEtBQUssQ0FBQyxNQUFNLEVBQUU7Z0JBQ2hCLElBQUksQ0FBQyxPQUFPLEdBQUcsQ0FBQzt3QkFDZCxRQUFRLEVBQUUsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLFVBQVUsQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLEdBQUc7d0JBQzVDLFNBQVMsRUFBRSxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsVUFBVSxDQUFDLEtBQUssQ0FBQyxJQUFJLEdBQUcsQ0FBQzt3QkFDN0MsUUFBUSxFQUFFLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxVQUFVLENBQUMsS0FBSyxDQUFDLEdBQUcsR0FBRyxDQUFDO3dCQUMzQyxPQUFPLEVBQUUsS0FBSyxDQUFDLEtBQUssQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDLENBQUMsVUFBVSxDQUFDLEdBQUcsQ0FBQyxJQUFJLEdBQUcsQ0FBQzt3QkFDeEQsTUFBTSxFQUFFLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxVQUFVLENBQUMsS0FBSyxDQUFDLEdBQUcsR0FBRyxDQUFDO3FCQUMx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QUVDO1lBREMsU0FBUyxDQUFDLFFBQVEsQ0FBQyxPQUFPLENBQUMsVUFBQSxLQUFLLElBQUksT0FBQSxLQUFLLENBQUMsS0FBSyxDQUFDLEtBQUksQ0FBQyxFQUFqQixDQUFpQixDQUFDLENBQUM7UUFDekQsQ0FBQztRQUVELGlDQUFRLEdBQVIsVUFBUyxHQUFRLEVBQUUsT0FBYTtZQUFoQyxpQkFFQztZQURDLE1BQU0sQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLEtBQUssQ0FBQyxDQUFDLE9BQU8sQ0FBQyxVQUFBLENBQUMsSUFBTSxHQUFHLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxLQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ3JFLENBQUM7UUFFRCw0Q0FBbUIsR0FBbkIsVUFBb0IsRUFBa0IsRUFBRSxPQUFhO1lBQXJELGlCQUVDO1lBREMsRUFBRSxDQUFDLFFBQVEsQ0FBQyxPQUFPLENBQUMsVUFBQSxLQUFLLElBQUksT0FBQSxLQUFLLENBQUMsS0FBSyxDQUFDLEtBQUksQ0FBQyxFQUFqQixDQUFpQixDQUFDLENBQUM7UUFDbEQsQ0FBQztRQUVELHlDQUFnQixHQUFoQixVQUFpQixFQUFlLEVBQUUsT0FBYSxJQUFRLENBQUM7UUFFeEQsNENBQW1CLEdBQW5CLFVBQW9CLEVBQWtCLEVBQUUsT0FBYSxJQUFRLENBQUM7UUFDaEUscUJBQUM7SUFBRCxDQUFDLEFBbEJELElBa0JDO0lBbEJZLHdDQUFjIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1BhcnNlU291cmNlU3Bhbn0gZnJvbSAnLi4vcGFyc2VfdXRpbCc7XG5cbmV4cG9ydCBjbGFzcyBNZXNzYWdlIHtcbiAgc291cmNlczogTWVzc2FnZVNwYW5bXTtcblxuICAvKipcbiAgICogQHBhcmFtIG5vZGVzIG1lc3NhZ2UgQVNUXG4gICAqIEBwYXJhbSBwbGFjZWhvbGRlcnMgbWFwcyBwbGFjZWhvbGRlciBuYW1lcyB0byBzdGF0aWMgY29udGVudFxuICAgKiBAcGFyYW0gcGxhY2Vob2xkZXJUb01lc3NhZ2UgbWFwcyBwbGFjZWhvbGRlciBuYW1lcyB0byBtZXNzYWdlcyAodXNlZCBmb3IgbmVzdGVkIElDVSBtZXNzYWdlcylcbiAgICogQHBhcmFtIG1lYW5pbmdcbiAgICogQHBhcmFtIGRlc2NyaXB0aW9uXG4gICAqIEBwYXJhbSBpZFxuICAgKi9cbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgbm9kZXM6IE5vZGVbXSwgcHVibGljIHBsYWNlaG9sZGVyczoge1twaE5hbWU6IHN0cmluZ106IHN0cmluZ30sXG4gICAgICBwdWJsaWMgcGxhY2Vob2xkZXJUb01lc3NhZ2U6IHtbcGhOYW1lOiBzdHJpbmddOiBNZXNzYWdlfSwgcHVibGljIG1lYW5pbmc6IHN0cmluZyxcbiAgICAgIHB1YmxpYyBkZXNjcmlwdGlvbjogc3RyaW5nLCBwdWJsaWMgaWQ6IHN0cmluZykge1xuICAgIGlmIChub2Rlcy5sZW5ndGgpIHtcbiAgICAgIHRoaXMuc291cmNlcyA9IFt7XG4gICAgICAgIGZpbGVQYXRoOiBub2Rlc1swXS5zb3VyY2VTcGFuLnN0YXJ0LmZpbGUudXJsLFxuICAgICAgICBzdGFydExpbmU6IG5vZGVzWzBdLnNvdXJjZVNwYW4uc3RhcnQubGluZSArIDEsXG4gICAgICAgIHN0YXJ0Q29sOiBub2Rlc1swXS5zb3VyY2VTcGFuLnN0YXJ0LmNvbCArIDEsXG4gICAgICAgIGVuZExpbmU6IG5vZGVzW25vZGVzLmxlbmd0aCAtIDFdLnNvdXJjZVNwYW4uZW5kLmxpbmUgKyAxLFxuICAgICAgICBlbmRDb2w6IG5vZGVzWzBdLnNvdXJjZVNwYW4uc3RhcnQuY29sICsgMVxuICAgICAgfV07XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuc291cmNlcyA9IFtdO1xuICAgIH1cbiAgfVxufVxuXG4vLyBsaW5lIGFuZCBjb2x1bW5zIGluZGV4ZXMgYXJlIDEgYmFzZWRcbmV4cG9ydCBpbnRlcmZhY2UgTWVzc2FnZVNwYW4ge1xuICBmaWxlUGF0aDogc3RyaW5nO1xuICBzdGFydExpbmU6IG51bWJlcjtcbiAgc3RhcnRDb2w6IG51bWJlcjtcbiAgZW5kTGluZTogbnVtYmVyO1xuICBlbmRDb2w6IG51bWJlcjtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBOb2RlIHtcbiAgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuO1xuICB2aXNpdCh2aXNpdG9yOiBWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55O1xufVxuXG5leHBvcnQgY2xhc3MgVGV4dCBpbXBsZW1lbnRzIE5vZGUge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgdmFsdWU6IHN0cmluZywgcHVibGljIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3Bhbikge31cblxuICB2aXNpdCh2aXNpdG9yOiBWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRUZXh0KHRoaXMsIGNvbnRleHQpOyB9XG59XG5cbi8vIFRPRE8odmljYik6IGRvIHdlIHJlYWxseSBuZWVkIHRoaXMgbm9kZSAodnMgYW4gYXJyYXkpID9cbmV4cG9ydCBjbGFzcyBDb250YWluZXIgaW1wbGVtZW50cyBOb2RlIHtcbiAgY29uc3RydWN0b3IocHVibGljIGNoaWxkcmVuOiBOb2RlW10sIHB1YmxpYyBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4pIHt9XG5cbiAgdmlzaXQodmlzaXRvcjogVmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7IHJldHVybiB2aXNpdG9yLnZpc2l0Q29udGFpbmVyKHRoaXMsIGNvbnRleHQpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBJY3UgaW1wbGVtZW50cyBOb2RlIHtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHB1YmxpYyBleHByZXNzaW9uUGxhY2Vob2xkZXIgITogc3RyaW5nO1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBleHByZXNzaW9uOiBzdHJpbmcsIHB1YmxpYyB0eXBlOiBzdHJpbmcsIHB1YmxpYyBjYXNlczoge1trOiBzdHJpbmddOiBOb2RlfSxcbiAgICAgIHB1YmxpYyBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4pIHt9XG5cbiAgdmlzaXQodmlzaXRvcjogVmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7IHJldHVybiB2aXNpdG9yLnZpc2l0SWN1KHRoaXMsIGNvbnRleHQpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBUYWdQbGFjZWhvbGRlciBpbXBsZW1lbnRzIE5vZGUge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyB0YWc6IHN0cmluZywgcHVibGljIGF0dHJzOiB7W2s6IHN0cmluZ106IHN0cmluZ30sIHB1YmxpYyBzdGFydE5hbWU6IHN0cmluZyxcbiAgICAgIHB1YmxpYyBjbG9zZU5hbWU6IHN0cmluZywgcHVibGljIGNoaWxkcmVuOiBOb2RlW10sIHB1YmxpYyBpc1ZvaWQ6IGJvb2xlYW4sXG4gICAgICBwdWJsaWMgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuKSB7fVxuXG4gIHZpc2l0KHZpc2l0b3I6IFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkgeyByZXR1cm4gdmlzaXRvci52aXNpdFRhZ1BsYWNlaG9sZGVyKHRoaXMsIGNvbnRleHQpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBQbGFjZWhvbGRlciBpbXBsZW1lbnRzIE5vZGUge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgdmFsdWU6IHN0cmluZywgcHVibGljIG5hbWU6IHN0cmluZywgcHVibGljIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3Bhbikge31cblxuICB2aXNpdCh2aXNpdG9yOiBWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRQbGFjZWhvbGRlcih0aGlzLCBjb250ZXh0KTsgfVxufVxuXG5leHBvcnQgY2xhc3MgSWN1UGxhY2Vob2xkZXIgaW1wbGVtZW50cyBOb2RlIHtcbiAgY29uc3RydWN0b3IocHVibGljIHZhbHVlOiBJY3UsIHB1YmxpYyBuYW1lOiBzdHJpbmcsIHB1YmxpYyBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4pIHt9XG5cbiAgdmlzaXQodmlzaXRvcjogVmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7IHJldHVybiB2aXNpdG9yLnZpc2l0SWN1UGxhY2Vob2xkZXIodGhpcywgY29udGV4dCk7IH1cbn1cblxuZXhwb3J0IHR5cGUgQVNUID0gTWVzc2FnZSB8IE5vZGU7XG5cbmV4cG9ydCBpbnRlcmZhY2UgVmlzaXRvciB7XG4gIHZpc2l0VGV4dCh0ZXh0OiBUZXh0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENvbnRhaW5lcihjb250YWluZXI6IENvbnRhaW5lciwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRJY3UoaWN1OiBJY3UsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0VGFnUGxhY2Vob2xkZXIocGg6IFRhZ1BsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdFBsYWNlaG9sZGVyKHBoOiBQbGFjZWhvbGRlciwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRJY3VQbGFjZWhvbGRlcihwaDogSWN1UGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG59XG5cbi8vIENsb25lIHRoZSBBU1RcbmV4cG9ydCBjbGFzcyBDbG9uZVZpc2l0b3IgaW1wbGVtZW50cyBWaXNpdG9yIHtcbiAgdmlzaXRUZXh0KHRleHQ6IFRleHQsIGNvbnRleHQ/OiBhbnkpOiBUZXh0IHsgcmV0dXJuIG5ldyBUZXh0KHRleHQudmFsdWUsIHRleHQuc291cmNlU3Bhbik7IH1cblxuICB2aXNpdENvbnRhaW5lcihjb250YWluZXI6IENvbnRhaW5lciwgY29udGV4dD86IGFueSk6IENvbnRhaW5lciB7XG4gICAgY29uc3QgY2hpbGRyZW4gPSBjb250YWluZXIuY2hpbGRyZW4ubWFwKG4gPT4gbi52aXNpdCh0aGlzLCBjb250ZXh0KSk7XG4gICAgcmV0dXJuIG5ldyBDb250YWluZXIoY2hpbGRyZW4sIGNvbnRhaW5lci5zb3VyY2VTcGFuKTtcbiAgfVxuXG4gIHZpc2l0SWN1KGljdTogSWN1LCBjb250ZXh0PzogYW55KTogSWN1IHtcbiAgICBjb25zdCBjYXNlczoge1trOiBzdHJpbmddOiBOb2RlfSA9IHt9O1xuICAgIE9iamVjdC5rZXlzKGljdS5jYXNlcykuZm9yRWFjaChrZXkgPT4gY2FzZXNba2V5XSA9IGljdS5jYXNlc1trZXldLnZpc2l0KHRoaXMsIGNvbnRleHQpKTtcbiAgICBjb25zdCBtc2cgPSBuZXcgSWN1KGljdS5leHByZXNzaW9uLCBpY3UudHlwZSwgY2FzZXMsIGljdS5zb3VyY2VTcGFuKTtcbiAgICBtc2cuZXhwcmVzc2lvblBsYWNlaG9sZGVyID0gaWN1LmV4cHJlc3Npb25QbGFjZWhvbGRlcjtcbiAgICByZXR1cm4gbXNnO1xuICB9XG5cbiAgdmlzaXRUYWdQbGFjZWhvbGRlcihwaDogVGFnUGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiBUYWdQbGFjZWhvbGRlciB7XG4gICAgY29uc3QgY2hpbGRyZW4gPSBwaC5jaGlsZHJlbi5tYXAobiA9PiBuLnZpc2l0KHRoaXMsIGNvbnRleHQpKTtcbiAgICByZXR1cm4gbmV3IFRhZ1BsYWNlaG9sZGVyKFxuICAgICAgICBwaC50YWcsIHBoLmF0dHJzLCBwaC5zdGFydE5hbWUsIHBoLmNsb3NlTmFtZSwgY2hpbGRyZW4sIHBoLmlzVm9pZCwgcGguc291cmNlU3Bhbik7XG4gIH1cblxuICB2aXNpdFBsYWNlaG9sZGVyKHBoOiBQbGFjZWhvbGRlciwgY29udGV4dD86IGFueSk6IFBsYWNlaG9sZGVyIHtcbiAgICByZXR1cm4gbmV3IFBsYWNlaG9sZGVyKHBoLnZhbHVlLCBwaC5uYW1lLCBwaC5zb3VyY2VTcGFuKTtcbiAgfVxuXG4gIHZpc2l0SWN1UGxhY2Vob2xkZXIocGg6IEljdVBsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogSWN1UGxhY2Vob2xkZXIge1xuICAgIHJldHVybiBuZXcgSWN1UGxhY2Vob2xkZXIocGgudmFsdWUsIHBoLm5hbWUsIHBoLnNvdXJjZVNwYW4pO1xuICB9XG59XG5cbi8vIFZpc2l0IGFsbCB0aGUgbm9kZXMgcmVjdXJzaXZlbHlcbmV4cG9ydCBjbGFzcyBSZWN1cnNlVmlzaXRvciBpbXBsZW1lbnRzIFZpc2l0b3Ige1xuICB2aXNpdFRleHQodGV4dDogVGV4dCwgY29udGV4dD86IGFueSk6IGFueSB7fVxuXG4gIHZpc2l0Q29udGFpbmVyKGNvbnRhaW5lcjogQ29udGFpbmVyLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICBjb250YWluZXIuY2hpbGRyZW4uZm9yRWFjaChjaGlsZCA9PiBjaGlsZC52aXNpdCh0aGlzKSk7XG4gIH1cblxuICB2aXNpdEljdShpY3U6IEljdSwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgT2JqZWN0LmtleXMoaWN1LmNhc2VzKS5mb3JFYWNoKGsgPT4geyBpY3UuY2FzZXNba10udmlzaXQodGhpcyk7IH0pO1xuICB9XG5cbiAgdmlzaXRUYWdQbGFjZWhvbGRlcihwaDogVGFnUGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHBoLmNoaWxkcmVuLmZvckVhY2goY2hpbGQgPT4gY2hpbGQudmlzaXQodGhpcykpO1xuICB9XG5cbiAgdmlzaXRQbGFjZWhvbGRlcihwaDogUGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiBhbnkge31cblxuICB2aXNpdEljdVBsYWNlaG9sZGVyKHBoOiBJY3VQbGFjZWhvbGRlciwgY29udGV4dD86IGFueSk6IGFueSB7fVxufVxuIl19