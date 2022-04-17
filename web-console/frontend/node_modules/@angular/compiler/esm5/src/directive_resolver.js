/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { createComponent, createContentChild, createContentChildren, createDirective, createHostBinding, createHostListener, createInput, createOutput, createViewChild, createViewChildren } from './core';
import { resolveForwardRef, splitAtColon, stringify } from './util';
var QUERY_METADATA_IDENTIFIERS = [
    createViewChild,
    createViewChildren,
    createContentChild,
    createContentChildren,
];
/*
 * Resolve a `Type` for {@link Directive}.
 *
 * This interface can be overridden by the application developer to create custom behavior.
 *
 * See {@link Compiler}
 */
var DirectiveResolver = /** @class */ (function () {
    function DirectiveResolver(_reflector) {
        this._reflector = _reflector;
    }
    DirectiveResolver.prototype.isDirective = function (type) {
        var typeMetadata = this._reflector.annotations(resolveForwardRef(type));
        return typeMetadata && typeMetadata.some(isDirectiveMetadata);
    };
    DirectiveResolver.prototype.resolve = function (type, throwIfNotFound) {
        if (throwIfNotFound === void 0) { throwIfNotFound = true; }
        var typeMetadata = this._reflector.annotations(resolveForwardRef(type));
        if (typeMetadata) {
            var metadata = findLast(typeMetadata, isDirectiveMetadata);
            if (metadata) {
                var propertyMetadata = this._reflector.propMetadata(type);
                var guards = this._reflector.guards(type);
                return this._mergeWithPropertyMetadata(metadata, propertyMetadata, guards, type);
            }
        }
        if (throwIfNotFound) {
            throw new Error("No Directive annotation found on " + stringify(type));
        }
        return null;
    };
    DirectiveResolver.prototype._mergeWithPropertyMetadata = function (dm, propertyMetadata, guards, directiveType) {
        var inputs = [];
        var outputs = [];
        var host = {};
        var queries = {};
        Object.keys(propertyMetadata).forEach(function (propName) {
            var input = findLast(propertyMetadata[propName], function (a) { return createInput.isTypeOf(a); });
            if (input) {
                if (input.bindingPropertyName) {
                    inputs.push(propName + ": " + input.bindingPropertyName);
                }
                else {
                    inputs.push(propName);
                }
            }
            var output = findLast(propertyMetadata[propName], function (a) { return createOutput.isTypeOf(a); });
            if (output) {
                if (output.bindingPropertyName) {
                    outputs.push(propName + ": " + output.bindingPropertyName);
                }
                else {
                    outputs.push(propName);
                }
            }
            var hostBindings = propertyMetadata[propName].filter(function (a) { return createHostBinding.isTypeOf(a); });
            hostBindings.forEach(function (hostBinding) {
                if (hostBinding.hostPropertyName) {
                    var startWith = hostBinding.hostPropertyName[0];
                    if (startWith === '(') {
                        throw new Error("@HostBinding can not bind to events. Use @HostListener instead.");
                    }
                    else if (startWith === '[') {
                        throw new Error("@HostBinding parameter should be a property name, 'class.<name>', or 'attr.<name>'.");
                    }
                    host["[" + hostBinding.hostPropertyName + "]"] = propName;
                }
                else {
                    host["[" + propName + "]"] = propName;
                }
            });
            var hostListeners = propertyMetadata[propName].filter(function (a) { return createHostListener.isTypeOf(a); });
            hostListeners.forEach(function (hostListener) {
                var args = hostListener.args || [];
                host["(" + hostListener.eventName + ")"] = propName + "(" + args.join(',') + ")";
            });
            var query = findLast(propertyMetadata[propName], function (a) { return QUERY_METADATA_IDENTIFIERS.some(function (i) { return i.isTypeOf(a); }); });
            if (query) {
                queries[propName] = query;
            }
        });
        return this._merge(dm, inputs, outputs, host, queries, guards, directiveType);
    };
    DirectiveResolver.prototype._extractPublicName = function (def) { return splitAtColon(def, [null, def])[1].trim(); };
    DirectiveResolver.prototype._dedupeBindings = function (bindings) {
        var names = new Set();
        var publicNames = new Set();
        var reversedResult = [];
        // go last to first to allow later entries to overwrite previous entries
        for (var i = bindings.length - 1; i >= 0; i--) {
            var binding = bindings[i];
            var name_1 = this._extractPublicName(binding);
            publicNames.add(name_1);
            if (!names.has(name_1)) {
                names.add(name_1);
                reversedResult.push(binding);
            }
        }
        return reversedResult.reverse();
    };
    DirectiveResolver.prototype._merge = function (directive, inputs, outputs, host, queries, guards, directiveType) {
        var mergedInputs = this._dedupeBindings(directive.inputs ? directive.inputs.concat(inputs) : inputs);
        var mergedOutputs = this._dedupeBindings(directive.outputs ? directive.outputs.concat(outputs) : outputs);
        var mergedHost = directive.host ? tslib_1.__assign({}, directive.host, host) : host;
        var mergedQueries = directive.queries ? tslib_1.__assign({}, directive.queries, queries) : queries;
        if (createComponent.isTypeOf(directive)) {
            var comp = directive;
            return createComponent({
                selector: comp.selector,
                inputs: mergedInputs,
                outputs: mergedOutputs,
                host: mergedHost,
                exportAs: comp.exportAs,
                moduleId: comp.moduleId,
                queries: mergedQueries,
                changeDetection: comp.changeDetection,
                providers: comp.providers,
                viewProviders: comp.viewProviders,
                entryComponents: comp.entryComponents,
                template: comp.template,
                templateUrl: comp.templateUrl,
                styles: comp.styles,
                styleUrls: comp.styleUrls,
                encapsulation: comp.encapsulation,
                animations: comp.animations,
                interpolation: comp.interpolation,
                preserveWhitespaces: directive.preserveWhitespaces,
            });
        }
        else {
            return createDirective({
                selector: directive.selector,
                inputs: mergedInputs,
                outputs: mergedOutputs,
                host: mergedHost,
                exportAs: directive.exportAs,
                queries: mergedQueries,
                providers: directive.providers, guards: guards
            });
        }
    };
    return DirectiveResolver;
}());
export { DirectiveResolver };
function isDirectiveMetadata(type) {
    return createDirective.isTypeOf(type) || createComponent.isTypeOf(type);
}
export function findLast(arr, condition) {
    for (var i = arr.length - 1; i >= 0; i--) {
        if (condition(arr[i])) {
            return arr[i];
        }
    }
    return null;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGlyZWN0aXZlX3Jlc29sdmVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2RpcmVjdGl2ZV9yZXNvbHZlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBR0gsT0FBTyxFQUE2QixlQUFlLEVBQUUsa0JBQWtCLEVBQUUscUJBQXFCLEVBQUUsZUFBZSxFQUFFLGlCQUFpQixFQUFFLGtCQUFrQixFQUFFLFdBQVcsRUFBRSxZQUFZLEVBQUUsZUFBZSxFQUFFLGtCQUFrQixFQUFDLE1BQU0sUUFBUSxDQUFDO0FBQ3RPLE9BQU8sRUFBQyxpQkFBaUIsRUFBRSxZQUFZLEVBQUUsU0FBUyxFQUFDLE1BQU0sUUFBUSxDQUFDO0FBRWxFLElBQU0sMEJBQTBCLEdBQUc7SUFDakMsZUFBZTtJQUNmLGtCQUFrQjtJQUNsQixrQkFBa0I7SUFDbEIscUJBQXFCO0NBQ3RCLENBQUM7QUFFRjs7Ozs7O0dBTUc7QUFDSDtJQUNFLDJCQUFvQixVQUE0QjtRQUE1QixlQUFVLEdBQVYsVUFBVSxDQUFrQjtJQUFHLENBQUM7SUFFcEQsdUNBQVcsR0FBWCxVQUFZLElBQVU7UUFDcEIsSUFBTSxZQUFZLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxXQUFXLENBQUMsaUJBQWlCLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQztRQUMxRSxPQUFPLFlBQVksSUFBSSxZQUFZLENBQUMsSUFBSSxDQUFDLG1CQUFtQixDQUFDLENBQUM7SUFDaEUsQ0FBQztJQVFELG1DQUFPLEdBQVAsVUFBUSxJQUFVLEVBQUUsZUFBc0I7UUFBdEIsZ0NBQUEsRUFBQSxzQkFBc0I7UUFDeEMsSUFBTSxZQUFZLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxXQUFXLENBQUMsaUJBQWlCLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQztRQUMxRSxJQUFJLFlBQVksRUFBRTtZQUNoQixJQUFN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QUMsVUFBVTtnQkFDM0IsYUFBYSxFQUFFLElBQUksQ0FBQyxhQUFhO2dCQUNqQyxtQkFBbUIsRUFBRSxTQUFTLENBQUMsbUJBQW1CO2FBQ25ELENBQUMsQ0FBQztTQUNKO2FBQU07WUFDTCxPQUFPLGVBQWUsQ0FBQztnQkFDckIsUUFBUSxFQUFFLFNBQVMsQ0FBQyxRQUFRO2dCQUM1QixNQUFNLEVBQUUsWUFBWTtnQkFDcEIsT0FBTyxFQUFFLGFBQWE7Z0JBQ3RCLElBQUksRUFBRSxVQUFVO2dCQUNoQixRQUFRLEVBQUUsU0FBUyxDQUFDLFFBQVE7Z0JBQzVCLE9BQU8sRUFBRSxhQUFhO2dCQUN0QixTQUFTLEVBQUUsU0FBUyxDQUFDLFNBQVMsRUFBRSxNQUFNLFFBQUE7YUFDdkMsQ0FBQyxDQUFDO1NBQ0o7SUFDSCxDQUFDO0lBQ0gsd0JBQUM7QUFBRCxDQUFDLEFBcEpELElBb0pDOztBQUVELFNBQVMsbUJBQW1CLENBQUMsSUFBUztJQUNwQyxPQUFPLGVBQWUsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLElBQUksZUFBZSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsQ0FBQztBQUMxRSxDQUFDO0FBRUQsTUFBTSxVQUFVLFFBQVEsQ0FBSSxHQUFRLEVBQUUsU0FBZ0M7SUFDcEUsS0FBSyxJQUFJLENBQUMsR0FBRyxHQUFHLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRSxDQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFO1FBQ3hDLElBQUksU0FBUyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxFQUFFO1lBQ3JCLE9BQU8sR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDO1NBQ2Y7S0FDRjtJQUNELE9BQU8sSUFBSSxDQUFDO0FBQ2QsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21waWxlUmVmbGVjdG9yfSBmcm9tICcuL2NvbXBpbGVfcmVmbGVjdG9yJztcbmltcG9ydCB7Q29tcG9uZW50LCBEaXJlY3RpdmUsIFR5cGUsIGNyZWF0ZUNvbXBvbmVudCwgY3JlYXRlQ29udGVudENoaWxkLCBjcmVhdGVDb250ZW50Q2hpbGRyZW4sIGNyZWF0ZURpcmVjdGl2ZSwgY3JlYXRlSG9zdEJpbmRpbmcsIGNyZWF0ZUhvc3RMaXN0ZW5lciwgY3JlYXRlSW5wdXQsIGNyZWF0ZU91dHB1dCwgY3JlYXRlVmlld0NoaWxkLCBjcmVhdGVWaWV3Q2hpbGRyZW59IGZyb20gJy4vY29yZSc7XG5pbXBvcnQge3Jlc29sdmVGb3J3YXJkUmVmLCBzcGxpdEF0Q29sb24sIHN0cmluZ2lmeX0gZnJvbSAnLi91dGlsJztcblxuY29uc3QgUVVFUllfTUVUQURBVEFfSURFTlRJRklFUlMgPSBbXG4gIGNyZWF0ZVZpZXdDaGlsZCxcbiAgY3JlYXRlVmlld0NoaWxkcmVuLFxuICBjcmVhdGVDb250ZW50Q2hpbGQsXG4gIGNyZWF0ZUNvbnRlbnRDaGlsZHJlbixcbl07XG5cbi8qXG4gKiBSZXNvbHZlIGEgYFR5cGVgIGZvciB7QGxpbmsgRGlyZWN0aXZlfS5cbiAqXG4gKiBUaGlzIGludGVyZmFjZSBjYW4gYmUgb3ZlcnJpZGRlbiBieSB0aGUgYXBwbGljYXRpb24gZGV2ZWxvcGVyIHRvIGNyZWF0ZSBjdXN0b20gYmVoYXZpb3IuXG4gKlxuICogU2VlIHtAbGluayBDb21waWxlcn1cbiAqL1xuZXhwb3J0IGNsYXNzIERpcmVjdGl2ZVJlc29sdmVyIHtcbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfcmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yKSB7fVxuXG4gIGlzRGlyZWN0aXZlKHR5cGU6IFR5cGUpIHtcbiAgICBjb25zdCB0eXBlTWV0YWRhdGEgPSB0aGlzLl9yZWZsZWN0b3IuYW5ub3RhdGlvbnMocmVzb2x2ZUZvcndhcmRSZWYodHlwZSkpO1xuICAgIHJldHVybiB0eXBlTWV0YWRhdGEgJiYgdHlwZU1ldGFkYXRhLnNvbWUoaXNEaXJlY3RpdmVNZXRhZGF0YSk7XG4gIH1cblxuICAvKipcbiAgICogUmV0dXJuIHtAbGluayBEaXJlY3RpdmV9IGZvciBhIGdpdmVuIGBUeXBlYC5cbiAgICovXG4gIHJlc29sdmUodHlwZTogVHlwZSk6IERpcmVjdGl2ZTtcbiAgcmVzb2x2ZSh0eXBlOiBUeXBlLCB0aHJvd0lmTm90Rm91bmQ6IHRydWUpOiBEaXJlY3RpdmU7XG4gIHJlc29sdmUodHlwZTogVHlwZSwgdGhyb3dJZk5vdEZvdW5kOiBib29sZWFuKTogRGlyZWN0aXZlfG51bGw7XG4gIHJlc29sdmUodHlwZTogVHlwZSwgdGhyb3dJZk5vdEZvdW5kID0gdHJ1ZSk6IERpcmVjdGl2ZXxudWxsIHtcbiAgICBjb25zdCB0eXBlTWV0YWRhdGEgPSB0aGlzLl9yZWZsZWN0b3IuYW5ub3RhdGlvbnMocmVzb2x2ZUZvcndhcmRSZWYodHlwZSkpO1xuICAgIGlmICh0eXBlTWV0YWRhdGEpIHtcbiAgICAgIGNvbnN0IG1ldGFkYXRhID0gZmluZExhc3QodHlwZU1ldGFkYXRhLCBpc0RpcmVjdGl2ZU1ldGFkYXRhKTtcbiAgICAgIGlmIChtZXRhZGF0YSkge1xuICAgICAgICBjb25zdCBwcm9wZXJ0eU1ldGFkYXRhID0gdGhpcy5fcmVmbGVjdG9yLnByb3BNZXRhZGF0YSh0eXBlKTtcbiAgICAgICAgY29uc3QgZ3VhcmRzID0gdGhpcy5fcmVmbGVjdG9yLmd1YXJkcyh0eXBlKTtcbiAgICAgICAgcmV0dXJuIHRoaXMuX21lcmdlV2l0aFByb3BlcnR5TWV0YWRhdGEobWV0YWRhdGEsIHByb3BlcnR5TWV0YWRhdGEsIGd1YXJkcywgdHlwZSk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKHRocm93SWZOb3RGb3VuZCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBObyBEaXJlY3RpdmUgYW5ub3RhdGlvbiBmb3VuZCBvbiAke3N0cmluZ2lmeSh0eXBlKX1gKTtcbiAgICB9XG5cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHByaXZhdGUgX21lcmdlV2l0aFByb3BlcnR5TWV0YWRhdGEoXG4gICAgICBkbTogRGlyZWN0aXZlLCBwcm9wZXJ0eU1ldGFkYXRhOiB7W2tleTogc3RyaW5nXTogYW55W119LCBndWFyZHM6IHtba2V5OiBzdHJpbmddOiBhbnl9LFxuICAgICAgZGlyZWN0aXZlVHlwZTogVHlwZSk6IERpcmVjdGl2ZSB7XG4gICAgY29uc3QgaW5wdXRzOiBzdHJpbmdbXSA9IFtdO1xuICAgIGNvbnN0IG91dHB1dHM6IHN0cmluZ1tdID0gW107XG4gICAgY29uc3QgaG9zdDoge1trZXk6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcbiAgICBjb25zdCBxdWVyaWVzOiB7W2tleTogc3RyaW5nXTogYW55fSA9IHt9O1xuICAgIE9iamVjdC5rZXlzKHByb3BlcnR5TWV0YWRhdGEpLmZvckVhY2goKHByb3BOYW1lOiBzdHJpbmcpID0+IHtcbiAgICAgIGNvbnN0IGlucHV0ID0gZmluZExhc3QocHJvcGVydHlNZXRhZGF0YVtwcm9wTmFtZV0sIChhKSA9PiBjcmVhdGVJbnB1dC5pc1R5cGVPZihhKSk7XG4gICAgICBpZiAoaW5wdXQpIHtcbiAgICAgICAgaWYgKGlucHV0LmJpbmRpbmdQcm9wZXJ0eU5hbWUpIHtcbiAgICAgICAgICBpbnB1dHMucHVzaChgJHtwcm9wTmFtZX06ICR7aW5wdXQuYmluZGluZ1Byb3BlcnR5TmFtZX1gKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBpbnB1dHMucHVzaChwcm9wTmFtZSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIGNvbnN0IG91dHB1dCA9IGZpbmRMYXN0KHByb3BlcnR5TWV0YWRhdGFbcHJvcE5hbWVdLCAoYSkgPT4gY3JlYXRlT3V0cHV0LmlzVHlwZU9mKGEpKTtcbiAgICAgIGlmIChvdXRwdXQpIHtcbiAgICAgICAgaWYgKG91dHB1dC5iaW5kaW5nUHJvcGVydHlOYW1lKSB7XG4gICAgICAgICAgb3V0cHV0cy5wdXNoKGAke3Byb3BOYW1lfTogJHtvdXRwdXQuYmluZGluZ1Byb3BlcnR5TmFtZX1gKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBvdXRwdXRzLnB1c2gocHJvcE5hbWUpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICBjb25zdCBob3N0QmluZGluZ3MgPSBwcm9wZXJ0eU1ldGFkYXRhW3Byb3BOYW1lXS5maWx0ZXIoYSA9PiBjcmVhdGVIb3N0QmluZGluZy5pc1R5cGVPZihhKSk7XG4gICAgICBob3N0QmluZGluZ3MuZm9yRWFjaChob3N0QmluZGluZyA9PiB7XG4gICAgICAgIGlmIChob3N0QmluZGluZy5ob3N0UHJvcGVydHlOYW1lKSB7XG4gICAgICAgICAgY29uc3Qgc3RhcnRXaXRoID0gaG9zdEJpbmRpbmcuaG9zdFByb3BlcnR5TmFtZVswXTtcbiAgICAgICAgICBpZiAoc3RhcnRXaXRoID09PSAnKCcpIHtcbiAgICAgICAgICAgIHRocm93IG5ldyBFcnJvcihgQEhvc3RCaW5kaW5nIGNhbiBub3QgYmluZCB0byBldmVudHMuIFVzZSBASG9zdExpc3RlbmVyIGluc3RlYWQuYCk7XG4gICAgICAgICAgfSBlbHNlIGlmIChzdGFydFdpdGggPT09ICdbJykge1xuICAgICAgICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgICAgICAgIGBASG9zdEJpbmRpbmcgcGFyYW1ldGVyIHNob3VsZCBiZSBhIHByb3BlcnR5IG5hbWUsICdjbGFzcy48bmFtZT4nLCBvciAnYXR0ci48bmFtZT4nLmApO1xuICAgICAgICAgIH1cbiAgICAgICAgICBob3N0W2BbJHtob3N0QmluZGluZy5ob3N0UHJvcGVydHlOYW1lfV1gXSA9IHByb3BOYW1lO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGhvc3RbYFske3Byb3BOYW1lfV1gXSA9IHByb3BOYW1lO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICAgIGNvbnN0IGhvc3RMaXN0ZW5lcnMgPSBwcm9wZXJ0eU1ldGFkYXRhW3Byb3BOYW1lXS5maWx0ZXIoYSA9PiBjcmVhdGVIb3N0TGlzdGVuZXIuaXNUeXBlT2YoYSkpO1xuICAgICAgaG9zdExpc3RlbmVycy5mb3JFYWNoKGhvc3RMaXN0ZW5lciA9PiB7XG4gICAgICAgIGNvbnN0IGFyZ3MgPSBob3N0TGlzdGVuZXIuYXJncyB8fCBbXTtcbiAgICAgICAgaG9zdFtgKCR7aG9zdExpc3RlbmVyLmV2ZW50TmFtZX0pYF0gPSBgJHtwcm9wTmFtZX0oJHthcmdzLmpvaW4oJywnKX0pYDtcbiAgICAgIH0pO1xuICAgICAgY29uc3QgcXVlcnkgPSBmaW5kTGFzdChcbiAgICAgICAgICBwcm9wZXJ0eU1ldGFkYXRhW3Byb3BOYW1lXSwgKGEpID0+IFFVRVJZX01FVEFEQVRBX0lERU5USUZJRVJTLnNvbWUoaSA9PiBpLmlzVHlwZU9mKGEpKSk7XG4gICAgICBpZiAocXVlcnkpIHtcbiAgICAgICAgcXVlcmllc1twcm9wTmFtZV0gPSBxdWVyeTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICByZXR1cm4gdGhpcy5fbWVyZ2UoZG0sIGlucHV0cywgb3V0cHV0cywgaG9zdCwgcXVlcmllcywgZ3VhcmRzLCBkaXJlY3RpdmVUeXBlKTtcbiAgfVxuXG4gIHByaXZhdGUgX2V4dHJhY3RQdWJsaWNOYW1lKGRlZjogc3RyaW5nKSB7IHJldHVybiBzcGxpdEF0Q29sb24oZGVmLCBbbnVsbCAhLCBkZWZdKVsxXS50cmltKCk7IH1cblxuICBwcml2YXRlIF9kZWR1cGVCaW5kaW5ncyhiaW5kaW5nczogc3RyaW5nW10pOiBzdHJpbmdbXSB7XG4gICAgY29uc3QgbmFtZXMgPSBuZXcgU2V0PHN0cmluZz4oKTtcbiAgICBjb25zdCBwdWJsaWNOYW1lcyA9IG5ldyBTZXQ8c3RyaW5nPigpO1xuICAgIGNvbnN0IHJldmVyc2VkUmVzdWx0OiBzdHJpbmdbXSA9IFtdO1xuICAgIC8vIGdvIGxhc3QgdG8gZmlyc3QgdG8gYWxsb3cgbGF0ZXIgZW50cmllcyB0byBvdmVyd3JpdGUgcHJldmlvdXMgZW50cmllc1xuICAgIGZvciAobGV0IGkgPSBiaW5kaW5ncy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgICAgY29uc3QgYmluZGluZyA9IGJpbmRpbmdzW2ldO1xuICAgICAgY29uc3QgbmFtZSA9IHRoaXMuX2V4dHJhY3RQdWJsaWNOYW1lKGJpbmRpbmcpO1xuICAgICAgcHVibGljTmFtZXMuYWRkKG5hbWUpO1xuICAgICAgaWYgKCFuYW1lcy5oYXMobmFtZSkpIHtcbiAgICAgICAgbmFtZXMuYWRkKG5hbWUpO1xuICAgICAgICByZXZlcnNlZFJlc3VsdC5wdXNoKGJpbmRpbmcpO1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gcmV2ZXJzZWRSZXN1bHQucmV2ZXJzZSgpO1xuICB9XG5cbiAgcHJpdmF0ZSBfbWVyZ2UoXG4gICAgICBkaXJlY3RpdmU6IERpcmVjdGl2ZSwgaW5wdXRzOiBzdHJpbmdbXSwgb3V0cHV0czogc3RyaW5nW10sIGhvc3Q6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9LFxuICAgICAgcXVlcmllczoge1trZXk6IHN0cmluZ106IGFueX0sIGd1YXJkczoge1trZXk6IHN0cmluZ106IGFueX0sIGRpcmVjdGl2ZVR5cGU6IFR5cGUpOiBEaXJlY3RpdmUge1xuICAgIGNvbnN0IG1lcmdlZElucHV0cyA9XG4gICAgICAgIHRoaXMuX2RlZHVwZUJpbmRpbmdzKGRpcmVjdGl2ZS5pbnB1dHMgPyBkaXJlY3RpdmUuaW5wdXRzLmNvbmNhdChpbnB1dHMpIDogaW5wdXRzKTtcbiAgICBjb25zdCBtZXJnZWRPdXRwdXRzID1cbiAgICAgICAgdGhpcy5fZGVkdXBlQmluZGluZ3MoZGlyZWN0aXZlLm91dHB1dHMgPyBkaXJlY3RpdmUub3V0cHV0cy5jb25jYXQob3V0cHV0cykgOiBvdXRwdXRzKTtcbiAgICBjb25zdCBtZXJnZWRIb3N0ID0gZGlyZWN0aXZlLmhvc3QgPyB7Li4uZGlyZWN0aXZlLmhvc3QsIC4uLmhvc3R9IDogaG9zdDtcbiAgICBjb25zdCBtZXJnZWRRdWVyaWVzID0gZGlyZWN0aXZlLnF1ZXJpZXMgPyB7Li4uZGlyZWN0aXZlLnF1ZXJpZXMsIC4uLnF1ZXJpZXN9IDogcXVlcmllcztcbiAgICBpZiAoY3JlYXRlQ29tcG9uZW50LmlzVHlwZU9mKGRpcmVjdGl2ZSkpIHtcbiAgICAgIGNvbnN0IGNvbXAgPSBkaXJlY3RpdmUgYXMgQ29tcG9uZW50O1xuICAgICAgcmV0dXJuIGNyZWF0ZUNvbXBvbmVudCh7XG4gICAgICAgIHNlbGVjdG9yOiBjb21wLnNlbGVjdG9yLFxuICAgICAgICBpbnB1dHM6IG1lcmdlZElucHV0cyxcbiAgICAgICAgb3V0cHV0czogbWVyZ2VkT3V0cHV0cyxcbiAgICAgICAgaG9zdDogbWVyZ2VkSG9zdCxcbiAgICAgICAgZXhwb3J0QXM6IGNvbXAuZXhwb3J0QXMsXG4gICAgICAgIG1vZHVsZUlkOiBjb21wLm1vZHVsZUlkLFxuICAgICAgICBxdWVyaWVzOiBtZXJnZWRRdWVyaWVzLFxuICAgICAgICBjaGFuZ2VEZXRlY3Rpb246IGNvbXAuY2hhbmdlRGV0ZWN0aW9uLFxuICAgICAgICBwcm92aWRlcnM6IGNvbXAucHJvdmlkZXJzLFxuICAgICAgICB2aWV3UHJvdmlkZXJzOiBjb21wLnZpZXdQcm92aWRlcnMsXG4gICAgICAgIGVudHJ5Q29tcG9uZW50czogY29tcC5lbnRyeUNvbXBvbmVudHMsXG4gICAgICAgIHRlbXBsYXRlOiBjb21wLnRlbXBsYXRlLFxuICAgICAgICB0ZW1wbGF0ZVVybDogY29tcC50ZW1wbGF0ZVVybCxcbiAgICAgICAgc3R5bGVzOiBjb21wLnN0eWxlcyxcbiAgICAgICAgc3R5bGVVcmxzOiBjb21wLnN0eWxlVXJscyxcbiAgICAgICAgZW5jYXBzdWxhdGlvbjogY29tcC5lbmNhcHN1bGF0aW9uLFxuICAgICAgICBhbmltYXRpb25zOiBjb21wLmFuaW1hdGlvbnMsXG4gICAgICAgIGludGVycG9sYXRpb246IGNvbXAuaW50ZXJwb2xhdGlvbixcbiAgICAgICAgcHJlc2VydmVXaGl0ZXNwYWNlczogZGlyZWN0aXZlLnByZXNlcnZlV2hpdGVzcGFjZXMsXG4gICAgICB9KTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIGNyZWF0ZURpcmVjdGl2ZSh7XG4gICAgICAgIHNlbGVjdG9yOiBkaXJlY3RpdmUuc2VsZWN0b3IsXG4gICAgICAgIGlucHV0czogbWVyZ2VkSW5wdXRzLFxuICAgICAgICBvdXRwdXRzOiBtZXJnZWRPdXRwdXRzLFxuICAgICAgICBob3N0OiBtZXJnZWRIb3N0LFxuICAgICAgICBleHBvcnRBczogZGlyZWN0aXZlLmV4cG9ydEFzLFxuICAgICAgICBxdWVyaWVzOiBtZXJnZWRRdWVyaWVzLFxuICAgICAgICBwcm92aWRlcnM6IGRpcmVjdGl2ZS5wcm92aWRlcnMsIGd1YXJkc1xuICAgICAgfSk7XG4gICAgfVxuICB9XG59XG5cbmZ1bmN0aW9uIGlzRGlyZWN0aXZlTWV0YWRhdGEodHlwZTogYW55KTogdHlwZSBpcyBEaXJlY3RpdmUge1xuICByZXR1cm4gY3JlYXRlRGlyZWN0aXZlLmlzVHlwZU9mKHR5cGUpIHx8IGNyZWF0ZUNvbXBvbmVudC5pc1R5cGVPZih0eXBlKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZpbmRMYXN0PFQ+KGFycjogVFtdLCBjb25kaXRpb246ICh2YWx1ZTogVCkgPT4gYm9vbGVhbik6IFR8bnVsbCB7XG4gIGZvciAobGV0IGkgPSBhcnIubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAoY29uZGl0aW9uKGFycltpXSkpIHtcbiAgICAgIHJldHVybiBhcnJbaV07XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuIl19