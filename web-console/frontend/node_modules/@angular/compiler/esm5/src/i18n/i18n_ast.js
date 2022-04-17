/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
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
export { Message };
var Text = /** @class */ (function () {
    function Text(value, sourceSpan) {
        this.value = value;
        this.sourceSpan = sourceSpan;
    }
    Text.prototype.visit = function (visitor, context) { return visitor.visitText(this, context); };
    return Text;
}());
export { Text };
// TODO(vicb): do we really need this node (vs an array) ?
var Container = /** @class */ (function () {
    function Container(children, sourceSpan) {
        this.children = children;
        this.sourceSpan = sourceSpan;
    }
    Container.prototype.visit = function (visitor, context) { return visitor.visitContainer(this, context); };
    return Container;
}());
export { Container };
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
export { Icu };
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
export { TagPlaceholder };
var Placeholder = /** @class */ (function () {
    function Placeholder(value, name, sourceSpan) {
        this.value = value;
        this.name = name;
        this.sourceSpan = sourceSpan;
    }
    Placeholder.prototype.visit = function (visitor, context) { return visitor.visitPlaceholder(this, context); };
    return Placeholder;
}());
export { Placeholder };
var IcuPlaceholder = /** @class */ (function () {
    function IcuPlaceholder(value, name, sourceSpan) {
        this.value = value;
        this.name = name;
        this.sourceSpan = sourceSpan;
    }
    IcuPlaceholder.prototype.visit = function (visitor, context) { return visitor.visitIcuPlaceholder(this, context); };
    return IcuPlaceholder;
}());
export { IcuPlaceholder };
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
export { CloneVisitor };
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
export { RecurseVisitor };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaTE4bl9hc3QuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvaTE4bi9pMThuX2FzdC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFJSDtJQUdFOzs7Ozs7O09BT0c7SUFDSCxpQkFDVyxLQUFhLEVBQVMsWUFBd0MsRUFDOUQsb0JBQWlELEVBQVMsT0FBZSxFQUN6RSxXQUFtQixFQUFTLEVBQVU7UUFGdEMsVUFBSyxHQUFMLEtBQUssQ0FBUTtRQUFTLGlCQUFZLEdBQVosWUFBWSxDQUE0QjtRQUM5RCx5QkFBb0IsR0FBcEIsb0JBQW9CLENBQTZCO1FBQVMsWUFBTyxHQUFQLE9BQU8sQ0FBUTtRQUN6RSxnQkFBVyxHQUFYLFdBQVcsQ0FBUTtRQUFTLE9BQUUsR0FBRixFQUFFLENBQVE7UUFDL0MsSUFBSSxLQUFLLENBQUMsTUFBTSxFQUFFO1lBQ2hCLElBQUksQ0FBQyxPQUFPLEdBQUcsQ0FBQztvQkFDZCxRQUFRLEVBQUUsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLFVBQVUsQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLEdBQUc7b0JBQzVDLFNBQVMsRUFBRSxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsVUFBVSxDQUFDLEtBQUssQ0FBQyxJQUFJLEdBQUcsQ0FBQztvQkFDN0MsUUFBUSxFQUFFLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxVQUFVLENBQUMsS0FBSyxDQUFDLEdBQUcsR0FBRyxDQUFDO29CQUMzQyxPQUFPLEVBQUUsS0FBSyxDQUFDLEtBQUssQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDLENBQUMsVUFBVSxDQUFDLEdBQUcsQ0FBQyxJQUFJLEdBQUcsQ0FBQztvQkFDeEQsTUFBTSxFQUFFLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxVQUFVLENBQUMsS0FBSyxDQUFDLEdBQUcsR0FBRyxDQUFDO2lCQUMxQyxD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QWlCLENBQUMsQ0FBQztJQUN6RCxDQUFDO0lBRUQsaUNBQVEsR0FBUixVQUFTLEdBQVEsRUFBRSxPQUFhO1FBQWhDLGlCQUVDO1FBREMsTUFBTSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDLENBQUMsT0FBTyxDQUFDLFVBQUEsQ0FBQyxJQUFNLEdBQUcsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLEtBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDckUsQ0FBQztJQUVELDRDQUFtQixHQUFuQixVQUFvQixFQUFrQixFQUFFLE9BQWE7UUFBckQsaUJBRUM7UUFEQyxFQUFFLENBQUMsUUFBUSxDQUFDLE9BQU8sQ0FBQyxVQUFBLEtBQUssSUFBSSxPQUFBLEtBQUssQ0FBQyxLQUFLLENBQUMsS0FBSSxDQUFDLEVBQWpCLENBQWlCLENBQUMsQ0FBQztJQUNsRCxDQUFDO0lBRUQseUNBQWdCLEdBQWhCLFVBQWlCLEVBQWUsRUFBRSxPQUFhLElBQVEsQ0FBQztJQUV4RCw0Q0FBbUIsR0FBbkIsVUFBb0IsRUFBa0IsRUFBRSxPQUFhLElBQVEsQ0FBQztJQUNoRSxxQkFBQztBQUFELENBQUMsQUFsQkQsSUFrQkMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7UGFyc2VTb3VyY2VTcGFufSBmcm9tICcuLi9wYXJzZV91dGlsJztcblxuZXhwb3J0IGNsYXNzIE1lc3NhZ2Uge1xuICBzb3VyY2VzOiBNZXNzYWdlU3BhbltdO1xuXG4gIC8qKlxuICAgKiBAcGFyYW0gbm9kZXMgbWVzc2FnZSBBU1RcbiAgICogQHBhcmFtIHBsYWNlaG9sZGVycyBtYXBzIHBsYWNlaG9sZGVyIG5hbWVzIHRvIHN0YXRpYyBjb250ZW50XG4gICAqIEBwYXJhbSBwbGFjZWhvbGRlclRvTWVzc2FnZSBtYXBzIHBsYWNlaG9sZGVyIG5hbWVzIHRvIG1lc3NhZ2VzICh1c2VkIGZvciBuZXN0ZWQgSUNVIG1lc3NhZ2VzKVxuICAgKiBAcGFyYW0gbWVhbmluZ1xuICAgKiBAcGFyYW0gZGVzY3JpcHRpb25cbiAgICogQHBhcmFtIGlkXG4gICAqL1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBub2RlczogTm9kZVtdLCBwdWJsaWMgcGxhY2Vob2xkZXJzOiB7W3BoTmFtZTogc3RyaW5nXTogc3RyaW5nfSxcbiAgICAgIHB1YmxpYyBwbGFjZWhvbGRlclRvTWVzc2FnZToge1twaE5hbWU6IHN0cmluZ106IE1lc3NhZ2V9LCBwdWJsaWMgbWVhbmluZzogc3RyaW5nLFxuICAgICAgcHVibGljIGRlc2NyaXB0aW9uOiBzdHJpbmcsIHB1YmxpYyBpZDogc3RyaW5nKSB7XG4gICAgaWYgKG5vZGVzLmxlbmd0aCkge1xuICAgICAgdGhpcy5zb3VyY2VzID0gW3tcbiAgICAgICAgZmlsZVBhdGg6IG5vZGVzWzBdLnNvdXJjZVNwYW4uc3RhcnQuZmlsZS51cmwsXG4gICAgICAgIHN0YXJ0TGluZTogbm9kZXNbMF0uc291cmNlU3Bhbi5zdGFydC5saW5lICsgMSxcbiAgICAgICAgc3RhcnRDb2w6IG5vZGVzWzBdLnNvdXJjZVNwYW4uc3RhcnQuY29sICsgMSxcbiAgICAgICAgZW5kTGluZTogbm9kZXNbbm9kZXMubGVuZ3RoIC0gMV0uc291cmNlU3Bhbi5lbmQubGluZSArIDEsXG4gICAgICAgIGVuZENvbDogbm9kZXNbMF0uc291cmNlU3Bhbi5zdGFydC5jb2wgKyAxXG4gICAgICB9XTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5zb3VyY2VzID0gW107XG4gICAgfVxuICB9XG59XG5cbi8vIGxpbmUgYW5kIGNvbHVtbnMgaW5kZXhlcyBhcmUgMSBiYXNlZFxuZXhwb3J0IGludGVyZmFjZSBNZXNzYWdlU3BhbiB7XG4gIGZpbGVQYXRoOiBzdHJpbmc7XG4gIHN0YXJ0TGluZTogbnVtYmVyO1xuICBzdGFydENvbDogbnVtYmVyO1xuICBlbmRMaW5lOiBudW1iZXI7XG4gIGVuZENvbDogbnVtYmVyO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIE5vZGUge1xuICBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW47XG4gIHZpc2l0KHZpc2l0b3I6IFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG59XG5cbmV4cG9ydCBjbGFzcyBUZXh0IGltcGxlbWVudHMgTm9kZSB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyB2YWx1ZTogc3RyaW5nLCBwdWJsaWMgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuKSB7fVxuXG4gIHZpc2l0KHZpc2l0b3I6IFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkgeyByZXR1cm4gdmlzaXRvci52aXNpdFRleHQodGhpcywgY29udGV4dCk7IH1cbn1cblxuLy8gVE9ETyh2aWNiKTogZG8gd2UgcmVhbGx5IG5lZWQgdGhpcyBub2RlICh2cyBhbiBhcnJheSkgP1xuZXhwb3J0IGNsYXNzIENvbnRhaW5lciBpbXBsZW1lbnRzIE5vZGUge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgY2hpbGRyZW46IE5vZGVbXSwgcHVibGljIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3Bhbikge31cblxuICB2aXNpdCh2aXNpdG9yOiBWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRDb250YWluZXIodGhpcywgY29udGV4dCk7IH1cbn1cblxuZXhwb3J0IGNsYXNzIEljdSBpbXBsZW1lbnRzIE5vZGUge1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHVibGljIGV4cHJlc3Npb25QbGFjZWhvbGRlciAhOiBzdHJpbmc7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIGV4cHJlc3Npb246IHN0cmluZywgcHVibGljIHR5cGU6IHN0cmluZywgcHVibGljIGNhc2VzOiB7W2s6IHN0cmluZ106IE5vZGV9LFxuICAgICAgcHVibGljIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3Bhbikge31cblxuICB2aXNpdCh2aXNpdG9yOiBWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRJY3UodGhpcywgY29udGV4dCk7IH1cbn1cblxuZXhwb3J0IGNsYXNzIFRhZ1BsYWNlaG9sZGVyIGltcGxlbWVudHMgTm9kZSB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIHRhZzogc3RyaW5nLCBwdWJsaWMgYXR0cnM6IHtbazogc3RyaW5nXTogc3RyaW5nfSwgcHVibGljIHN0YXJ0TmFtZTogc3RyaW5nLFxuICAgICAgcHVibGljIGNsb3NlTmFtZTogc3RyaW5nLCBwdWJsaWMgY2hpbGRyZW46IE5vZGVbXSwgcHVibGljIGlzVm9pZDogYm9vbGVhbixcbiAgICAgIHB1YmxpYyBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4pIHt9XG5cbiAgdmlzaXQodmlzaXRvcjogVmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7IHJldHVybiB2aXNpdG9yLnZpc2l0VGFnUGxhY2Vob2xkZXIodGhpcywgY29udGV4dCk7IH1cbn1cblxuZXhwb3J0IGNsYXNzIFBsYWNlaG9sZGVyIGltcGxlbWVudHMgTm9kZSB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyB2YWx1ZTogc3RyaW5nLCBwdWJsaWMgbmFtZTogc3RyaW5nLCBwdWJsaWMgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuKSB7fVxuXG4gIHZpc2l0KHZpc2l0b3I6IFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkgeyByZXR1cm4gdmlzaXRvci52aXNpdFBsYWNlaG9sZGVyKHRoaXMsIGNvbnRleHQpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBJY3VQbGFjZWhvbGRlciBpbXBsZW1lbnRzIE5vZGUge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgdmFsdWU6IEljdSwgcHVibGljIG5hbWU6IHN0cmluZywgcHVibGljIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3Bhbikge31cblxuICB2aXNpdCh2aXNpdG9yOiBWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRJY3VQbGFjZWhvbGRlcih0aGlzLCBjb250ZXh0KTsgfVxufVxuXG5leHBvcnQgdHlwZSBBU1QgPSBNZXNzYWdlIHwgTm9kZTtcblxuZXhwb3J0IGludGVyZmFjZSBWaXNpdG9yIHtcbiAgdmlzaXRUZXh0KHRleHQ6IFRleHQsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0Q29udGFpbmVyKGNvbnRhaW5lcjogQ29udGFpbmVyLCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdEljdShpY3U6IEljdSwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRUYWdQbGFjZWhvbGRlcihwaDogVGFnUGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0UGxhY2Vob2xkZXIocGg6IFBsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdEljdVBsYWNlaG9sZGVyKHBoOiBJY3VQbGFjZWhvbGRlciwgY29udGV4dD86IGFueSk6IGFueTtcbn1cblxuLy8gQ2xvbmUgdGhlIEFTVFxuZXhwb3J0IGNsYXNzIENsb25lVmlzaXRvciBpbXBsZW1lbnRzIFZpc2l0b3Ige1xuICB2aXNpdFRleHQodGV4dDogVGV4dCwgY29udGV4dD86IGFueSk6IFRleHQgeyByZXR1cm4gbmV3IFRleHQodGV4dC52YWx1ZSwgdGV4dC5zb3VyY2VTcGFuKTsgfVxuXG4gIHZpc2l0Q29udGFpbmVyKGNvbnRhaW5lcjogQ29udGFpbmVyLCBjb250ZXh0PzogYW55KTogQ29udGFpbmVyIHtcbiAgICBjb25zdCBjaGlsZHJlbiA9IGNvbnRhaW5lci5jaGlsZHJlbi5tYXAobiA9PiBuLnZpc2l0KHRoaXMsIGNvbnRleHQpKTtcbiAgICByZXR1cm4gbmV3IENvbnRhaW5lcihjaGlsZHJlbiwgY29udGFpbmVyLnNvdXJjZVNwYW4pO1xuICB9XG5cbiAgdmlzaXRJY3UoaWN1OiBJY3UsIGNvbnRleHQ/OiBhbnkpOiBJY3Uge1xuICAgIGNvbnN0IGNhc2VzOiB7W2s6IHN0cmluZ106IE5vZGV9ID0ge307XG4gICAgT2JqZWN0LmtleXMoaWN1LmNhc2VzKS5mb3JFYWNoKGtleSA9PiBjYXNlc1trZXldID0gaWN1LmNhc2VzW2tleV0udmlzaXQodGhpcywgY29udGV4dCkpO1xuICAgIGNvbnN0IG1zZyA9IG5ldyBJY3UoaWN1LmV4cHJlc3Npb24sIGljdS50eXBlLCBjYXNlcywgaWN1LnNvdXJjZVNwYW4pO1xuICAgIG1zZy5leHByZXNzaW9uUGxhY2Vob2xkZXIgPSBpY3UuZXhwcmVzc2lvblBsYWNlaG9sZGVyO1xuICAgIHJldHVybiBtc2c7XG4gIH1cblxuICB2aXNpdFRhZ1BsYWNlaG9sZGVyKHBoOiBUYWdQbGFjZWhvbGRlciwgY29udGV4dD86IGFueSk6IFRhZ1BsYWNlaG9sZGVyIHtcbiAgICBjb25zdCBjaGlsZHJlbiA9IHBoLmNoaWxkcmVuLm1hcChuID0+IG4udmlzaXQodGhpcywgY29udGV4dCkpO1xuICAgIHJldHVybiBuZXcgVGFnUGxhY2Vob2xkZXIoXG4gICAgICAgIHBoLnRhZywgcGguYXR0cnMsIHBoLnN0YXJ0TmFtZSwgcGguY2xvc2VOYW1lLCBjaGlsZHJlbiwgcGguaXNWb2lkLCBwaC5zb3VyY2VTcGFuKTtcbiAgfVxuXG4gIHZpc2l0UGxhY2Vob2xkZXIocGg6IFBsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogUGxhY2Vob2xkZXIge1xuICAgIHJldHVybiBuZXcgUGxhY2Vob2xkZXIocGgudmFsdWUsIHBoLm5hbWUsIHBoLnNvdXJjZVNwYW4pO1xuICB9XG5cbiAgdmlzaXRJY3VQbGFjZWhvbGRlcihwaDogSWN1UGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiBJY3VQbGFjZWhvbGRlciB7XG4gICAgcmV0dXJuIG5ldyBJY3VQbGFjZWhvbGRlcihwaC52YWx1ZSwgcGgubmFtZSwgcGguc291cmNlU3Bhbik7XG4gIH1cbn1cblxuLy8gVmlzaXQgYWxsIHRoZSBub2RlcyByZWN1cnNpdmVseVxuZXhwb3J0IGNsYXNzIFJlY3Vyc2VWaXNpdG9yIGltcGxlbWVudHMgVmlzaXRvciB7XG4gIHZpc2l0VGV4dCh0ZXh0OiBUZXh0LCBjb250ZXh0PzogYW55KTogYW55IHt9XG5cbiAgdmlzaXRDb250YWluZXIoY29udGFpbmVyOiBDb250YWluZXIsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIGNvbnRhaW5lci5jaGlsZHJlbi5mb3JFYWNoKGNoaWxkID0+IGNoaWxkLnZpc2l0KHRoaXMpKTtcbiAgfVxuXG4gIHZpc2l0SWN1KGljdTogSWN1LCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICBPYmplY3Qua2V5cyhpY3UuY2FzZXMpLmZvckVhY2goayA9PiB7IGljdS5jYXNlc1trXS52aXNpdCh0aGlzKTsgfSk7XG4gIH1cblxuICB2aXNpdFRhZ1BsYWNlaG9sZGVyKHBoOiBUYWdQbGFjZWhvbGRlciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcGguY2hpbGRyZW4uZm9yRWFjaChjaGlsZCA9PiBjaGlsZC52aXNpdCh0aGlzKSk7XG4gIH1cblxuICB2aXNpdFBsYWNlaG9sZGVyKHBoOiBQbGFjZWhvbGRlciwgY29udGV4dD86IGFueSk6IGFueSB7fVxuXG4gIHZpc2l0SWN1UGxhY2Vob2xkZXIocGg6IEljdVBsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogYW55IHt9XG59XG4iXX0=