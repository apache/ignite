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
        define("@angular/compiler/src/i18n/serializers/serializer", ["require", "exports", "tslib", "@angular/compiler/src/i18n/i18n_ast"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var i18n = require("@angular/compiler/src/i18n/i18n_ast");
    var Serializer = /** @class */ (function () {
        function Serializer() {
        }
        // Creates a name mapper, see `PlaceholderMapper`
        // Returning `null` means that no name mapping is used.
        Serializer.prototype.createNameMapper = function (message) { return null; };
        return Serializer;
    }());
    exports.Serializer = Serializer;
    /**
     * A simple mapper that take a function to transform an internal name to a public name
     */
    var SimplePlaceholderMapper = /** @class */ (function (_super) {
        tslib_1.__extends(SimplePlaceholderMapper, _super);
        // create a mapping from the message
        function SimplePlaceholderMapper(message, mapName) {
            var _this = _super.call(this) || this;
            _this.mapName = mapName;
            _this.internalToPublic = {};
            _this.publicToNextId = {};
            _this.publicToInternal = {};
            message.nodes.forEach(function (node) { return node.visit(_this); });
            return _this;
        }
        SimplePlaceholderMapper.prototype.toPublicName = function (internalName) {
            return this.internalToPublic.hasOwnProperty(internalName) ?
                this.internalToPublic[internalName] :
                null;
        };
        SimplePlaceholderMapper.prototype.toInternalName = function (publicName) {
            return this.publicToInternal.hasOwnProperty(publicName) ? this.publicToInternal[publicName] :
                null;
        };
        SimplePlaceholderMapper.prototype.visitText = function (text, context) { return null; };
        SimplePlaceholderMapper.prototype.visitTagPlaceholder = function (ph, context) {
            this.visitPlaceholderName(ph.startName);
            _super.prototype.visitTagPlaceholder.call(this, ph, context);
            this.visitPlaceholderName(ph.closeName);
        };
        SimplePlaceholderMapper.prototype.visitPlaceholder = function (ph, context) { this.visitPlaceholderName(ph.name); };
        SimplePlaceholderMapper.prototype.visitIcuPlaceholder = function (ph, context) {
            this.visitPlaceholderName(ph.name);
        };
        // XMB placeholders could only contains A-Z, 0-9 and _
        SimplePlaceholderMapper.prototype.visitPlaceholderName = function (internalName) {
            if (!internalName || this.internalToPublic.hasOwnProperty(internalName)) {
                return;
            }
            var publicName = this.mapName(internalName);
            if (this.publicToInternal.hasOwnProperty(publicName)) {
                // Create a new XMB when it has already been used
                var nextId = this.publicToNextId[publicName];
                this.publicToNextId[publicName] = nextId + 1;
                publicName = publicName + "_" + nextId;
            }
            else {
                this.publicToNextId[publicName] = 1;
            }
            this.internalToPublic[internalName] = publicName;
            this.publicToInternal[publicName] = internalName;
        };
        return SimplePlaceholderMapper;
    }(i18n.RecurseVisitor));
    exports.SimplePlaceholderMapper = SimplePlaceholderMapper;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2VyaWFsaXplci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9pMThuL3NlcmlhbGl6ZXJzL3NlcmlhbGl6ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7O0lBRUgsMERBQW9DO0lBRXBDO1FBQUE7UUFjQSxDQUFDO1FBSEMsaURBQWlEO1FBQ2pELHVEQUF1RDtRQUN2RCxxQ0FBZ0IsR0FBaEIsVUFBaUIsT0FBcUIsSUFBNEIsT0FBTyxJQUFJLENBQUMsQ0FBQyxDQUFDO1FBQ2xGLGlCQUFDO0lBQUQsQ0FBQyxBQWRELElBY0M7SUFkcUIsZ0NBQVU7SUE0QmhDOztPQUVHO0lBQ0g7UUFBNkMsbURBQW1CO1FBSzlELG9DQUFvQztRQUNwQyxpQ0FBWSxPQUFxQixFQUFVLE9BQWlDO1lBQTVFLFlBQ0UsaUJBQU8sU0FFUjtZQUgwQyxhQUFPLEdBQVAsT0FBTyxDQUEwQjtZQUxwRSxzQkFBZ0IsR0FBMEIsRUFBRSxDQUFDO1lBQzdDLG9CQUFjLEdBQTBCLEVBQUUsQ0FBQztZQUMzQyxzQkFBZ0IsR0FBMEIsRUFBRSxDQUFDO1lBS25ELE9BQU8sQ0FBQyxLQUFLLENBQUMsT0FBTyxDQUFDLFVBQUEsSUFBSSxJQUFJLE9BQUEsSUFBSSxDQUFDLEtBQUssQ0FBQyxLQUFJLENBQUMsRUFBaEIsQ0FBZ0IsQ0FBQyxDQUFDOztRQUNsRCxDQUFDO1FBRUQsOENBQVksR0FBWixVQUFhLFlBQW9CO1lBQy9CLE9BQU8sSUFBSSxDQUFDLGdCQUFnQixDQUFDLGNBQWMsQ0FBQyxZQUFZLENBQUMsQ0FBQyxDQUFDO2dCQUN2RCxJQUFJLENBQUMsZ0JBQWdCLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQztnQkFDckMsSUFBSSxDQUFDO1FBQ1gsQ0FBQztRQUVELGdEQUFjLEdBQWQsVUFBZSxVQUFrQjtZQUMvQixPQUFPLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxjQUFjLENBQUMsVUFBVSxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxVQUFVLENBQUMsQ0FBQyxDQUFDO2dCQUNuQyxJQUFJLENBQUM7UUFDakUsQ0FBQztRQUVELDJDQUFTLEdBQVQsVUFBVSxJQUFlLEVBQUUsT0FBYSxJQUFTLE9BQU8sSUFBSSxDQUFDLENBQUMsQ0FBQztRQUUvRCxxREFBbUIsR0FBbkIsVUFBb0IsRUFBdUIsRUFBRSxPQUFhO1lBQ3hELElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxFQUFFLENBQUMsU0FBUyxDQUFDLENBQUM7WUFDeEMsaUJBQU0sbUJBQW1CLFlBQUMsRUFBRSxFQUFFLE9BQU8sQ0FBQyxDQUFDO1lBQ3ZDLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxFQUFFLENBQUMsU0FBUyxDQUFDLENBQUM7UUFDMUMsQ0FBQztRQUVELGtEQUFnQixHQUFoQixVQUFpQixFQUFvQixFQUFFLE9BQWEsSUFBUyxJQUFJLENBQUMsb0JBQW9CLENBQUMsRUFBRSxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUVsRyxxREFBbUIsR0FBbkIsVUFBb0IsRUFBdUIsRUFBRSxPQUFhO1lBQ3hELElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxFQUFFLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDckMsQ0FBQztRQUVELHNEQUFzRDtRQUM5QyxzREFBb0IsR0FBNUIsVUFBNkIsWUFBb0I7WUFDL0MsSUFBSSxDQUFDLFlBQVksSUFBSSxJQUFJLENBQUMsZ0JBQWdCLENBQUMsY0FBYyxDQUFDLFlBQVksQ0FBQyxFQUFFO2dCQUN2RSxPQUFPO2FBQ1I7WUFFRCxJQUFJLFVBQVUsR0FBRyxJQUFJLENBQUMsT0FBTyxDQUFDLFlBQVksQ0FBQyxDQUFDO1lBRTVDLElBQUksSUFBSSxDQUFDLGdCQUFnQixDQUFDLGNBQWMsQ0FBQyxVQUFVLENBQUMsRUFBRTtnQkFDcEQsaURBQWlEO2dCQUNqRCxJQUFNLE1BQU0sR0FBRyxJQUFJLENBQUMsY0FBYyxDQUFDLFVBQVUsQ0FBQyxDQUFDO2dCQUMvQyxJQUFJLENBQUMsY0FBYyxDQUFDLFVBQVUsQ0FBQyxHQUFHLE1BQU0sR0FBRyxDQUFDLENBQUM7Z0JBQzdDLFVBQVUsR0FBTSxVQUFVLFNBQUksTUFBUSxDQUFDO2FBQ3hDO2lCQUFNO2dCQUNMLElBQUksQ0FBQyxjQUFjLENBQUMsVUFBVSxDQUFDLEdBQUcsQ0FBQyxDQUFDO2FBQ3JDO1lBRUQsSUFBSSxDQUFDLGdCQUFnQixDQUFDLFlBQVksQ0FBQyxHQUFHLFVBQVUsQ0FBQztZQUNqRCxJQUFJLENBQUMsZ0JBQWdCLENBQUMsVUFBVSxDQUFDLEdBQUcsWUFBWSxDQUFDO1FBQ25ELENBQUM7UUFDSCw4QkFBQztJQUFELENBQUMsQUF4REQsQ0FBNkMsSUFBSSxDQUFDLGNBQWMsR0F3RC9EO0lBeERZLDBEQUF1QiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0ICogYXMgaTE4biBmcm9tICcuLi9pMThuX2FzdCc7XG5cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBTZXJpYWxpemVyIHtcbiAgLy8gLSBUaGUgYHBsYWNlaG9sZGVyc2AgYW5kIGBwbGFjZWhvbGRlclRvTWVzc2FnZWAgcHJvcGVydGllcyBhcmUgaXJyZWxldmFudCBpbiB0aGUgaW5wdXQgbWVzc2FnZXNcbiAgLy8gLSBUaGUgYGlkYCBjb250YWlucyB0aGUgbWVzc2FnZSBpZCB0aGF0IHRoZSBzZXJpYWxpemVyIGlzIGV4cGVjdGVkIHRvIHVzZVxuICAvLyAtIFBsYWNlaG9sZGVyIG5hbWVzIGFyZSBhbHJlYWR5IG1hcCB0byBwdWJsaWMgbmFtZXMgdXNpbmcgdGhlIHByb3ZpZGVkIG1hcHBlclxuICBhYnN0cmFjdCB3cml0ZShtZXNzYWdlczogaTE4bi5NZXNzYWdlW10sIGxvY2FsZTogc3RyaW5nfG51bGwpOiBzdHJpbmc7XG5cbiAgYWJzdHJhY3QgbG9hZChjb250ZW50OiBzdHJpbmcsIHVybDogc3RyaW5nKTpcbiAgICAgIHtsb2NhbGU6IHN0cmluZyB8IG51bGwsIGkxOG5Ob2Rlc0J5TXNnSWQ6IHtbbXNnSWQ6IHN0cmluZ106IGkxOG4uTm9kZVtdfX07XG5cbiAgYWJzdHJhY3QgZGlnZXN0KG1lc3NhZ2U6IGkxOG4uTWVzc2FnZSk6IHN0cmluZztcblxuICAvLyBDcmVhdGVzIGEgbmFtZSBtYXBwZXIsIHNlZSBgUGxhY2Vob2xkZXJNYXBwZXJgXG4gIC8vIFJldHVybmluZyBgbnVsbGAgbWVhbnMgdGhhdCBubyBuYW1lIG1hcHBpbmcgaXMgdXNlZC5cbiAgY3JlYXRlTmFtZU1hcHBlcihtZXNzYWdlOiBpMThuLk1lc3NhZ2UpOiBQbGFjZWhvbGRlck1hcHBlcnxudWxsIHsgcmV0dXJuIG51bGw7IH1cbn1cblxuLyoqXG4gKiBBIGBQbGFjZWhvbGRlck1hcHBlcmAgY29udmVydHMgcGxhY2Vob2xkZXIgbmFtZXMgZnJvbSBpbnRlcm5hbCB0byBzZXJpYWxpemVkIHJlcHJlc2VudGF0aW9uIGFuZFxuICogYmFjay5cbiAqXG4gKiBJdCBzaG91bGQgYmUgdXNlZCBmb3Igc2VyaWFsaXphdGlvbiBmb3JtYXQgdGhhdCBwdXQgY29uc3RyYWludHMgb24gdGhlIHBsYWNlaG9sZGVyIG5hbWVzLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIFBsYWNlaG9sZGVyTWFwcGVyIHtcbiAgdG9QdWJsaWNOYW1lKGludGVybmFsTmFtZTogc3RyaW5nKTogc3RyaW5nfG51bGw7XG5cbiAgdG9JbnRlcm5hbE5hbWUocHVibGljTmFtZTogc3RyaW5nKTogc3RyaW5nfG51bGw7XG59XG5cbi8qKlxuICogQSBzaW1wbGUgbWFwcGVyIHRoYXQgdGFrZSBhIGZ1bmN0aW9uIHRvIHRyYW5zZm9ybSBhbiBpbnRlcm5hbCBuYW1lIHRvIGEgcHVibGljIG5hbWVcbiAqL1xuZXhwb3J0IGNsYXNzIFNpbXBsZVBsYWNlaG9sZGVyTWFwcGVyIGV4dGVuZHMgaTE4bi5SZWN1cnNlVmlzaXRvciBpbXBsZW1lbnRzIFBsYWNlaG9sZGVyTWFwcGVyIHtcbiAgcHJpdmF0ZSBpbnRlcm5hbFRvUHVibGljOiB7W2s6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcbiAgcHJpdmF0ZSBwdWJsaWNUb05leHRJZDoge1trOiBzdHJpbmddOiBudW1iZXJ9ID0ge307XG4gIHByaXZhdGUgcHVibGljVG9JbnRlcm5hbDoge1trOiBzdHJpbmddOiBzdHJpbmd9ID0ge307XG5cbiAgLy8gY3JlYXRlIGEgbWFwcGluZyBmcm9tIHRoZSBtZXNzYWdlXG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2U6IGkxOG4uTWVzc2FnZSwgcHJpdmF0ZSBtYXBOYW1lOiAobmFtZTogc3RyaW5nKSA9PiBzdHJpbmcpIHtcbiAgICBzdXBlcigpO1xuICAgIG1lc3NhZ2Uubm9kZXMuZm9yRWFjaChub2RlID0+IG5vZGUudmlzaXQodGhpcykpO1xuICB9XG5cbiAgdG9QdWJsaWNOYW1lKGludGVybmFsTmFtZTogc3RyaW5nKTogc3RyaW5nfG51bGwge1xuICAgIHJldHVybiB0aGlzLmludGVybmFsVG9QdWJsaWMuaGFzT3duUHJvcGVydHkoaW50ZXJuYWxOYW1lKSA/XG4gICAgICAgIHRoaXMuaW50ZXJuYWxUb1B1YmxpY1tpbnRlcm5hbE5hbWVdIDpcbiAgICAgICAgbnVsbDtcbiAgfVxuXG4gIHRvSW50ZXJuYWxOYW1lKHB1YmxpY05hbWU6IHN0cmluZyk6IHN0cmluZ3xudWxsIHtcbiAgICByZXR1cm4gdGhpcy5wdWJsaWNUb0ludGVybmFsLmhhc093blByb3BlcnR5KHB1YmxpY05hbWUpID8gdGhpcy5wdWJsaWNUb0ludGVybmFsW3B1YmxpY05hbWVdIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgbnVsbDtcbiAgfVxuXG4gIHZpc2l0VGV4dCh0ZXh0OiBpMThuLlRleHQsIGNvbnRleHQ/OiBhbnkpOiBhbnkgeyByZXR1cm4gbnVsbDsgfVxuXG4gIHZpc2l0VGFnUGxhY2Vob2xkZXIocGg6IGkxOG4uVGFnUGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHRoaXMudmlzaXRQbGFjZWhvbGRlck5hbWUocGguc3RhcnROYW1lKTtcbiAgICBzdXBlci52aXNpdFRhZ1BsYWNlaG9sZGVyKHBoLCBjb250ZXh0KTtcbiAgICB0aGlzLnZpc2l0UGxhY2Vob2xkZXJOYW1lKHBoLmNsb3NlTmFtZSk7XG4gIH1cblxuICB2aXNpdFBsYWNlaG9sZGVyKHBoOiBpMThuLlBsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogYW55IHsgdGhpcy52aXNpdFBsYWNlaG9sZGVyTmFtZShwaC5uYW1lKTsgfVxuXG4gIHZpc2l0SWN1UGxhY2Vob2xkZXIocGg6IGkxOG4uSWN1UGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHRoaXMudmlzaXRQbGFjZWhvbGRlck5hbWUocGgubmFtZSk7XG4gIH1cblxuICAvLyBYTUIgcGxhY2Vob2xkZXJzIGNvdWxkIG9ubHkgY29udGFpbnMgQS1aLCAwLTkgYW5kIF9cbiAgcHJpdmF0ZSB2aXNpdFBsYWNlaG9sZGVyTmFtZShpbnRlcm5hbE5hbWU6IHN0cmluZyk6IHZvaWQge1xuICAgIGlmICghaW50ZXJuYWxOYW1lIHx8IHRoaXMuaW50ZXJuYWxUb1B1YmxpYy5oYXNPd25Qcm9wZXJ0eShpbnRlcm5hbE5hbWUpKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgbGV0IHB1YmxpY05hbWUgPSB0aGlzLm1hcE5hbWUoaW50ZXJuYWxOYW1lKTtcblxuICAgIGlmICh0aGlzLnB1YmxpY1RvSW50ZXJuYWwuaGFzT3duUHJvcGVydHkocHVibGljTmFtZSkpIHtcbiAgICAgIC8vIENyZWF0ZSBhIG5ldyBYTUIgd2hlbiBpdCBoYXMgYWxyZWFkeSBiZWVuIHVzZWRcbiAgICAgIGNvbnN0IG5leHRJZCA9IHRoaXMucHVibGljVG9OZXh0SWRbcHVibGljTmFtZV07XG4gICAgICB0aGlzLnB1YmxpY1RvTmV4dElkW3B1YmxpY05hbWVdID0gbmV4dElkICsgMTtcbiAgICAgIHB1YmxpY05hbWUgPSBgJHtwdWJsaWNOYW1lfV8ke25leHRJZH1gO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnB1YmxpY1RvTmV4dElkW3B1YmxpY05hbWVdID0gMTtcbiAgICB9XG5cbiAgICB0aGlzLmludGVybmFsVG9QdWJsaWNbaW50ZXJuYWxOYW1lXSA9IHB1YmxpY05hbWU7XG4gICAgdGhpcy5wdWJsaWNUb0ludGVybmFsW3B1YmxpY05hbWVdID0gaW50ZXJuYWxOYW1lO1xuICB9XG59XG4iXX0=