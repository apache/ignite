/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Type, isType } from '../interface/type';
import { ANNOTATIONS, PARAMETERS, PROP_METADATA } from '../util/decorators';
import { global } from '../util/global';
import { stringify } from '../util/stringify';
/**
 * Attention: These regex has to hold even if the code is minified!
 */
export var DELEGATE_CTOR = /^function\s+\S+\(\)\s*{[\s\S]+\.apply\(this,\s*arguments\)/;
export var INHERITED_CLASS = /^class\s+[A-Za-z\d$_]*\s*extends\s+[^{]+{/;
export var INHERITED_CLASS_WITH_CTOR = /^class\s+[A-Za-z\d$_]*\s*extends\s+[^{]+{[\s\S]*constructor\s*\(/;
export var INHERITED_CLASS_WITH_DELEGATE_CTOR = /^class\s+[A-Za-z\d$_]*\s*extends\s+[^{]+{[\s\S]*constructor\s*\(\)\s*{\s+super\(\.\.\.arguments\)/;
/**
 * Determine whether a stringified type is a class which delegates its constructor
 * to its parent.
 *
 * This is not trivial since compiled code can actually contain a constructor function
 * even if the original source code did not. For instance, when the child class contains
 * an initialized instance property.
 */
export function isDelegateCtor(typeStr) {
    return DELEGATE_CTOR.test(typeStr) || INHERITED_CLASS_WITH_DELEGATE_CTOR.test(typeStr) ||
        (INHERITED_CLASS.test(typeStr) && !INHERITED_CLASS_WITH_CTOR.test(typeStr));
}
var ReflectionCapabilities = /** @class */ (function () {
    function ReflectionCapabilities(reflect) {
        this._reflect = reflect || global['Reflect'];
    }
    ReflectionCapabilities.prototype.isReflectionEnabled = function () { return true; };
    ReflectionCapabilities.prototype.factory = function (t) { return function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        return new (t.bind.apply(t, tslib_1.__spread([void 0], args)))();
    }; };
    /** @internal */
    ReflectionCapabilities.prototype._zipTypesAndAnnotations = function (paramTypes, paramAnnotations) {
        var result;
        if (typeof paramTypes === 'undefined') {
            result = new Array(paramAnnotations.length);
        }
        else {
            result = new Array(paramTypes.length);
        }
        for (var i = 0; i < result.length; i++) {
            // TS outputs Object for parameters without types, while Traceur omits
            // the annotations. For now we preserve the Traceur behavior to aid
            // migration, but this can be revisited.
            if (typeof paramTypes === 'undefined') {
                result[i] = [];
            }
            else if (paramTypes[i] && paramTypes[i] != Object) {
                result[i] = [paramTypes[i]];
            }
            else {
                result[i] = [];
            }
            if (paramAnnotations && paramAnnotations[i] != null) {
                result[i] = result[i].concat(paramAnnotations[i]);
            }
        }
        return result;
    };
    ReflectionCapabilities.prototype._ownParameters = function (type, parentCtor) {
        var typeStr = type.toString();
        // If we have no decorators, we only have function.length as metadata.
        // In that case, to detect whether a child class declared an own constructor or not,
        // we need to look inside of that constructor to check whether it is
        // just calling the parent.
        // This also helps to work around for https://github.com/Microsoft/TypeScript/issues/12439
        // that sets 'design:paramtypes' to []
        // if a class inherits from another class but has no ctor declared itself.
        if (isDelegateCtor(typeStr)) {
            return null;
        }
        // Prefer the direct API.
        if (type.parameters && type.parameters !== parentCtor.parameters) {
            return type.parameters;
        }
        // API of tsickle for lowering decorators to properties on the class.
        var tsickleCtorParams = type.ctorParameters;
        if (tsickleCtorParams && tsickleCtorParams !== parentCtor.ctorParameters) {
            // Newer tsickle uses a function closure
            // Retain the non-function case for compatibility with older tsickle
            var ctorParameters = typeof tsickleCtorParams === 'function' ? tsickleCtorParams() : tsickleCtorParams;
            var paramTypes_1 = ctorParameters.map(function (ctorParam) { return ctorParam && ctorParam.type; });
            var paramAnnotations_1 = ctorParameters.map(function (ctorParam) {
                return ctorParam && convertTsickleDecoratorIntoMetadata(ctorParam.decorators);
            });
            return this._zipTypesAndAnnotations(paramTypes_1, paramAnnotations_1);
        }
        // API for metadata created by invoking the decorators.
        var paramAnnotations = type.hasOwnProperty(PARAMETERS) && type[PARAMETERS];
        var paramTypes = this._reflect && this._reflect.getOwnMetadata &&
            this._reflect.getOwnMetadata('design:paramtypes', type);
        if (paramTypes || paramAnnotations) {
            return this._zipTypesAndAnnotations(paramTypes, paramAnnotations);
        }
        // If a class has no decorators, at least create metadata
        // based on function.length.
        // Note: We know that this is a real constructor as we checked
        // the content of the constructor above.
        return new Array(type.length).fill(undefined);
    };
    ReflectionCapabilities.prototype.parameters = function (type) {
        // Note: only report metadata if we have at least one class decorator
        // to stay in sync with the static reflector.
        if (!isType(type)) {
            return [];
        }
        var parentCtor = getParentCtor(type);
        var parameters = this._ownParameters(type, parentCtor);
        if (!parameters && parentCtor !== Object) {
            parameters = this.parameters(parentCtor);
        }
        return parameters || [];
    };
    ReflectionCapabilities.prototype._ownAnnotations = function (typeOrFunc, parentCtor) {
        // Prefer the direct API.
        if (typeOrFunc.annotations && typeOrFunc.annotations !== parentCtor.annotations) {
            var annotations = typeOrFunc.annotations;
            if (typeof annotations === 'function' && annotations.annotations) {
                annotations = annotations.annotations;
            }
            return annotations;
        }
        // API of tsickle for lowering decorators to properties on the class.
        if (typeOrFunc.decorators && typeOrFunc.decorators !== parentCtor.decorators) {
            return convertTsickleDecoratorIntoMetadata(typeOrFunc.decorators);
        }
        // API for metadata created by invoking the decorators.
        if (typeOrFunc.hasOwnProperty(ANNOTATIONS)) {
            return typeOrFunc[ANNOTATIONS];
        }
        return null;
    };
    ReflectionCapabilities.prototype.annotations = function (typeOrFunc) {
        if (!isType(typeOrFunc)) {
            return [];
        }
        var parentCtor = getParentCtor(typeOrFunc);
        var ownAnnotations = this._ownAnnotations(typeOrFunc, parentCtor) || [];
        var parentAnnotations = parentCtor !== Object ? this.annotations(parentCtor) : [];
        return parentAnnotations.concat(ownAnnotations);
    };
    ReflectionCapabilities.prototype._ownPropMetadata = function (typeOrFunc, parentCtor) {
        // Prefer the direct API.
        if (typeOrFunc.propMetadata &&
            typeOrFunc.propMetadata !== parentCtor.propMetadata) {
            var propMetadata = typeOrFunc.propMetadata;
            if (typeof propMetadata === 'function' && propMetadata.propMetadata) {
                propMetadata = propMetadata.propMetadata;
            }
            return propMetadata;
        }
        // API of tsickle for lowering decorators to properties on the class.
        if (typeOrFunc.propDecorators &&
            typeOrFunc.propDecorators !== parentCtor.propDecorators) {
            var propDecorators_1 = typeOrFunc.propDecorators;
            var propMetadata_1 = {};
            Object.keys(propDecorators_1).forEach(function (prop) {
                propMetadata_1[prop] = convertTsickleDecoratorIntoMetadata(propDecorators_1[prop]);
            });
            return propMetadata_1;
        }
        // API for metadata created by invoking the decorators.
        if (typeOrFunc.hasOwnProperty(PROP_METADATA)) {
            return typeOrFunc[PROP_METADATA];
        }
        return null;
    };
    ReflectionCapabilities.prototype.propMetadata = function (typeOrFunc) {
        if (!isType(typeOrFunc)) {
            return {};
        }
        var parentCtor = getParentCtor(typeOrFunc);
        var propMetadata = {};
        if (parentCtor !== Object) {
            var parentPropMetadata_1 = this.propMetadata(parentCtor);
            Object.keys(parentPropMetadata_1).forEach(function (propName) {
                propMetadata[propName] = parentPropMetadata_1[propName];
            });
        }
        var ownPropMetadata = this._ownPropMetadata(typeOrFunc, parentCtor);
        if (ownPropMetadata) {
            Object.keys(ownPropMetadata).forEach(function (propName) {
                var decorators = [];
                if (propMetadata.hasOwnProperty(propName)) {
                    decorators.push.apply(decorators, tslib_1.__spread(propMetadata[propName]));
                }
                decorators.push.apply(decorators, tslib_1.__spread(ownPropMetadata[propName]));
                propMetadata[propName] = decorators;
            });
        }
        return propMetadata;
    };
    ReflectionCapabilities.prototype.ownPropMetadata = function (typeOrFunc) {
        if (!isType(typeOrFunc)) {
            return {};
        }
        return this._ownPropMetadata(typeOrFunc, getParentCtor(typeOrFunc)) || {};
    };
    ReflectionCapabilities.prototype.hasLifecycleHook = function (type, lcProperty) {
        return type instanceof Type && lcProperty in type.prototype;
    };
    ReflectionCapabilities.prototype.guards = function (type) { return {}; };
    ReflectionCapabilities.prototype.getter = function (name) { return new Function('o', 'return o.' + name + ';'); };
    ReflectionCapabilities.prototype.setter = function (name) {
        return new Function('o', 'v', 'return o.' + name + ' = v;');
    };
    ReflectionCapabilities.prototype.method = function (name) {
        var functionBody = "if (!o." + name + ") throw new Error('\"" + name + "\" is undefined');\n        return o." + name + ".apply(o, args);";
        return new Function('o', 'args', functionBody);
    };
    // There is not a concept of import uri in Js, but this is useful in developing Dart applications.
    ReflectionCapabilities.prototype.importUri = function (type) {
        // StaticSymbol
        if (typeof type === 'object' && type['filePath']) {
            return type['filePath'];
        }
        // Runtime type
        return "./" + stringify(type);
    };
    ReflectionCapabilities.prototype.resourceUri = function (type) { return "./" + stringify(type); };
    ReflectionCapabilities.prototype.resolveIdentifier = function (name, moduleUrl, members, runtime) {
        return runtime;
    };
    ReflectionCapabilities.prototype.resolveEnum = function (enumIdentifier, name) { return enumIdentifier[name]; };
    return ReflectionCapabilities;
}());
export { ReflectionCapabilities };
function convertTsickleDecoratorIntoMetadata(decoratorInvocations) {
    if (!decoratorInvocations) {
        return [];
    }
    return decoratorInvocations.map(function (decoratorInvocation) {
        var decoratorType = decoratorInvocation.type;
        var annotationCls = decoratorType.annotationCls;
        var annotationArgs = decoratorInvocation.args ? decoratorInvocation.args : [];
        return new (annotationCls.bind.apply(annotationCls, tslib_1.__spread([void 0], annotationArgs)))();
    });
}
function getParentCtor(ctor) {
    var parentProto = ctor.prototype ? Object.getPrototypeOf(ctor.prototype) : null;
    var parentCtor = parentProto ? parentProto.constructor : null;
    // Note: We always use `Object` as the null value
    // to simplify checking later on.
    return parentCtor || Object;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVmbGVjdGlvbl9jYXBhYmlsaXRpZXMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZWZsZWN0aW9uL3JlZmxlY3Rpb25fY2FwYWJpbGl0aWVzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFFSCxPQUFPLEVBQUMsSUFBSSxFQUFFLE1BQU0sRUFBQyxNQUFNLG1CQUFtQixDQUFDO0FBQy9DLE9BQU8sRUFBQyxXQUFXLEVBQUUsVUFBVSxFQUFFLGFBQWEsRUFBQyxNQUFNLG9CQUFvQixDQUFDO0FBQzFFLE9BQU8sRUFBQyxNQUFNLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUN0QyxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFPNUM7O0dBRUc7QUFDSCxNQUFNLENBQUMsSUFBTSxhQUFhLEdBQUcsNERBQTRELENBQUM7QUFDMUYsTUFBTSxDQUFDLElBQU0sZUFBZSxHQUFHLDJDQUEyQyxDQUFDO0FBQzNFLE1BQU0sQ0FBQyxJQUFNLHlCQUF5QixHQUNsQyxrRUFBa0UsQ0FBQztBQUN2RSxNQUFNLENBQUMsSUFBTSxrQ0FBa0MsR0FDM0MsbUdBQW1HLENBQUM7QUFFeEc7Ozs7Ozs7R0FPRztBQUNILE1BQU0sVUFBVSxjQUFjLENBQUMsT0FBZTtJQUM1QyxPQUFPLGFBQWEsQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksa0NBQWtDLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQztRQUNsRixDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyx5QkFBeUIsQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FBQztBQUNsRixDQUFDO0FBRUQ7SUFHRSxnQ0FBWSxPQUFhO1FBQUksSUFBSSxDQUFDLFFBQVEsR0FBRyxPQUFPLElBQUksTUFBTSxDQUFDLFNBQVMs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QUMsSUFBSSxDQUFDO0lBQ2hFLGlEQUFpRDtJQUNqRCxpQ0FBaUM7SUFDakMsT0FBTyxVQUFVLElBQUksTUFBTSxDQUFDO0FBQzlCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7VHlwZSwgaXNUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2UvdHlwZSc7XG5pbXBvcnQge0FOTk9UQVRJT05TLCBQQVJBTUVURVJTLCBQUk9QX01FVEFEQVRBfSBmcm9tICcuLi91dGlsL2RlY29yYXRvcnMnO1xuaW1wb3J0IHtnbG9iYWx9IGZyb20gJy4uL3V0aWwvZ2xvYmFsJztcbmltcG9ydCB7c3RyaW5naWZ5fSBmcm9tICcuLi91dGlsL3N0cmluZ2lmeSc7XG5cbmltcG9ydCB7UGxhdGZvcm1SZWZsZWN0aW9uQ2FwYWJpbGl0aWVzfSBmcm9tICcuL3BsYXRmb3JtX3JlZmxlY3Rpb25fY2FwYWJpbGl0aWVzJztcbmltcG9ydCB7R2V0dGVyRm4sIE1ldGhvZEZuLCBTZXR0ZXJGbn0gZnJvbSAnLi90eXBlcyc7XG5cblxuXG4vKipcbiAqIEF0dGVudGlvbjogVGhlc2UgcmVnZXggaGFzIHRvIGhvbGQgZXZlbiBpZiB0aGUgY29kZSBpcyBtaW5pZmllZCFcbiAqL1xuZXhwb3J0IGNvbnN0IERFTEVHQVRFX0NUT1IgPSAvXmZ1bmN0aW9uXFxzK1xcUytcXChcXClcXHMqe1tcXHNcXFNdK1xcLmFwcGx5XFwodGhpcyxcXHMqYXJndW1lbnRzXFwpLztcbmV4cG9ydCBjb25zdCBJTkhFUklURURfQ0xBU1MgPSAvXmNsYXNzXFxzK1tBLVphLXpcXGQkX10qXFxzKmV4dGVuZHNcXHMrW157XSt7LztcbmV4cG9ydCBjb25zdCBJTkhFUklURURfQ0xBU1NfV0lUSF9DVE9SID1cbiAgICAvXmNsYXNzXFxzK1tBLVphLXpcXGQkX10qXFxzKmV4dGVuZHNcXHMrW157XSt7W1xcc1xcU10qY29uc3RydWN0b3JcXHMqXFwoLztcbmV4cG9ydCBjb25zdCBJTkhFUklURURfQ0xBU1NfV0lUSF9ERUxFR0FURV9DVE9SID1cbiAgICAvXmNsYXNzXFxzK1tBLVphLXpcXGQkX10qXFxzKmV4dGVuZHNcXHMrW157XSt7W1xcc1xcU10qY29uc3RydWN0b3JcXHMqXFwoXFwpXFxzKntcXHMrc3VwZXJcXChcXC5cXC5cXC5hcmd1bWVudHNcXCkvO1xuXG4vKipcbiAqIERldGVybWluZSB3aGV0aGVyIGEgc3RyaW5naWZpZWQgdHlwZSBpcyBhIGNsYXNzIHdoaWNoIGRlbGVnYXRlcyBpdHMgY29uc3RydWN0b3JcbiAqIHRvIGl0cyBwYXJlbnQuXG4gKlxuICogVGhpcyBpcyBub3QgdHJpdmlhbCBzaW5jZSBjb21waWxlZCBjb2RlIGNhbiBhY3R1YWxseSBjb250YWluIGEgY29uc3RydWN0b3IgZnVuY3Rpb25cbiAqIGV2ZW4gaWYgdGhlIG9yaWdpbmFsIHNvdXJjZSBjb2RlIGRpZCBub3QuIEZvciBpbnN0YW5jZSwgd2hlbiB0aGUgY2hpbGQgY2xhc3MgY29udGFpbnNcbiAqIGFuIGluaXRpYWxpemVkIGluc3RhbmNlIHByb3BlcnR5LlxuICovXG5leHBvcnQgZnVuY3Rpb24gaXNEZWxlZ2F0ZUN0b3IodHlwZVN0cjogc3RyaW5nKTogYm9vbGVhbiB7XG4gIHJldHVybiBERUxFR0FURV9DVE9SLnRlc3QodHlwZVN0cikgfHwgSU5IRVJJVEVEX0NMQVNTX1dJVEhfREVMRUdBVEVfQ1RPUi50ZXN0KHR5cGVTdHIpIHx8XG4gICAgICAoSU5IRVJJVEVEX0NMQVNTLnRlc3QodHlwZVN0cikgJiYgIUlOSEVSSVRFRF9DTEFTU19XSVRIX0NUT1IudGVzdCh0eXBlU3RyKSk7XG59XG5cbmV4cG9ydCBjbGFzcyBSZWZsZWN0aW9uQ2FwYWJpbGl0aWVzIGltcGxlbWVudHMgUGxhdGZvcm1SZWZsZWN0aW9uQ2FwYWJpbGl0aWVzIHtcbiAgcHJpdmF0ZSBfcmVmbGVjdDogYW55O1xuXG4gIGNvbnN0cnVjdG9yKHJlZmxlY3Q/OiBhbnkpIHsgdGhpcy5fcmVmbGVjdCA9IHJlZmxlY3QgfHwgZ2xvYmFsWydSZWZsZWN0J107IH1cblxuICBpc1JlZmxlY3Rpb25FbmFibGVkKCk6IGJvb2xlYW4geyByZXR1cm4gdHJ1ZTsgfVxuXG4gIGZhY3Rvcnk8VD4odDogVHlwZTxUPik6IChhcmdzOiBhbnlbXSkgPT4gVCB7IHJldHVybiAoLi4uYXJnczogYW55W10pID0+IG5ldyB0KC4uLmFyZ3MpOyB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfemlwVHlwZXNBbmRBbm5vdGF0aW9ucyhwYXJhbVR5cGVzOiBhbnlbXSwgcGFyYW1Bbm5vdGF0aW9uczogYW55W10pOiBhbnlbXVtdIHtcbiAgICBsZXQgcmVzdWx0OiBhbnlbXVtdO1xuXG4gICAgaWYgKHR5cGVvZiBwYXJhbVR5cGVzID09PSAndW5kZWZpbmVkJykge1xuICAgICAgcmVzdWx0ID0gbmV3IEFycmF5KHBhcmFtQW5ub3RhdGlvbnMubGVuZ3RoKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmVzdWx0ID0gbmV3IEFycmF5KHBhcmFtVHlwZXMubGVuZ3RoKTtcbiAgICB9XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHJlc3VsdC5sZW5ndGg7IGkrKykge1xuICAgICAgLy8gVFMgb3V0cHV0cyBPYmplY3QgZm9yIHBhcmFtZXRlcnMgd2l0aG91dCB0eXBlcywgd2hpbGUgVHJhY2V1ciBvbWl0c1xuICAgICAgLy8gdGhlIGFubm90YXRpb25zLiBGb3Igbm93IHdlIHByZXNlcnZlIHRoZSBUcmFjZXVyIGJlaGF2aW9yIHRvIGFpZFxuICAgICAgLy8gbWlncmF0aW9uLCBidXQgdGhpcyBjYW4gYmUgcmV2aXNpdGVkLlxuICAgICAgaWYgKHR5cGVvZiBwYXJhbVR5cGVzID09PSAndW5kZWZpbmVkJykge1xuICAgICAgICByZXN1bHRbaV0gPSBbXTtcbiAgICAgIH0gZWxzZSBpZiAocGFyYW1UeXBlc1tpXSAmJiBwYXJhbVR5cGVzW2ldICE9IE9iamVjdCkge1xuICAgICAgICByZXN1bHRbaV0gPSBbcGFyYW1UeXBlc1tpXV07XG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXN1bHRbaV0gPSBbXTtcbiAgICAgIH1cbiAgICAgIGlmIChwYXJhbUFubm90YXRpb25zICYmIHBhcmFtQW5ub3RhdGlvbnNbaV0gIT0gbnVsbCkge1xuICAgICAgICByZXN1bHRbaV0gPSByZXN1bHRbaV0uY29uY2F0KHBhcmFtQW5ub3RhdGlvbnNbaV0pO1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgcHJpdmF0ZSBfb3duUGFyYW1ldGVycyh0eXBlOiBUeXBlPGFueT4sIHBhcmVudEN0b3I6IGFueSk6IGFueVtdW118bnVsbCB7XG4gICAgY29uc3QgdHlwZVN0ciA9IHR5cGUudG9TdHJpbmcoKTtcbiAgICAvLyBJZiB3ZSBoYXZlIG5vIGRlY29yYXRvcnMsIHdlIG9ubHkgaGF2ZSBmdW5jdGlvbi5sZW5ndGggYXMgbWV0YWRhdGEuXG4gICAgLy8gSW4gdGhhdCBjYXNlLCB0byBkZXRlY3Qgd2hldGhlciBhIGNoaWxkIGNsYXNzIGRlY2xhcmVkIGFuIG93biBjb25zdHJ1Y3RvciBvciBub3QsXG4gICAgLy8gd2UgbmVlZCB0byBsb29rIGluc2lkZSBvZiB0aGF0IGNvbnN0cnVjdG9yIHRvIGNoZWNrIHdoZXRoZXIgaXQgaXNcbiAgICAvLyBqdXN0IGNhbGxpbmcgdGhlIHBhcmVudC5cbiAgICAvLyBUaGlzIGFsc28gaGVscHMgdG8gd29yayBhcm91bmQgZm9yIGh0dHBzOi8vZ2l0aHViLmNvbS9NaWNyb3NvZnQvVHlwZVNjcmlwdC9pc3N1ZXMvMTI0MzlcbiAgICAvLyB0aGF0IHNldHMgJ2Rlc2lnbjpwYXJhbXR5cGVzJyB0byBbXVxuICAgIC8vIGlmIGEgY2xhc3MgaW5oZXJpdHMgZnJvbSBhbm90aGVyIGNsYXNzIGJ1dCBoYXMgbm8gY3RvciBkZWNsYXJlZCBpdHNlbGYuXG4gICAgaWYgKGlzRGVsZWdhdGVDdG9yKHR5cGVTdHIpKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG5cbiAgICAvLyBQcmVmZXIgdGhlIGRpcmVjdCBBUEkuXG4gICAgaWYgKCg8YW55PnR5cGUpLnBhcmFtZXRlcnMgJiYgKDxhbnk+dHlwZSkucGFyYW1ldGVycyAhPT0gcGFyZW50Q3Rvci5wYXJhbWV0ZXJzKSB7XG4gICAgICByZXR1cm4gKDxhbnk+dHlwZSkucGFyYW1ldGVycztcbiAgICB9XG5cbiAgICAvLyBBUEkgb2YgdHNpY2tsZSBmb3IgbG93ZXJpbmcgZGVjb3JhdG9ycyB0byBwcm9wZXJ0aWVzIG9uIHRoZSBjbGFzcy5cbiAgICBjb25zdCB0c2lja2xlQ3RvclBhcmFtcyA9ICg8YW55PnR5cGUpLmN0b3JQYXJhbWV0ZXJzO1xuICAgIGlmICh0c2lja2xlQ3RvclBhcmFtcyAmJiB0c2lja2xlQ3RvclBhcmFtcyAhPT0gcGFyZW50Q3Rvci5jdG9yUGFyYW1ldGVycykge1xuICAgICAgLy8gTmV3ZXIgdHNpY2tsZSB1c2VzIGEgZnVuY3Rpb24gY2xvc3VyZVxuICAgICAgLy8gUmV0YWluIHRoZSBub24tZnVuY3Rpb24gY2FzZSBmb3IgY29tcGF0aWJpbGl0eSB3aXRoIG9sZGVyIHRzaWNrbGVcbiAgICAgIGNvbnN0IGN0b3JQYXJhbWV0ZXJzID1cbiAgICAgICAgICB0eXBlb2YgdHNpY2tsZUN0b3JQYXJhbXMgPT09ICdmdW5jdGlvbicgPyB0c2lja2xlQ3RvclBhcmFtcygpIDogdHNpY2tsZUN0b3JQYXJhbXM7XG4gICAgICBjb25zdCBwYXJhbVR5cGVzID0gY3RvclBhcmFtZXRlcnMubWFwKChjdG9yUGFyYW06IGFueSkgPT4gY3RvclBhcmFtICYmIGN0b3JQYXJhbS50eXBlKTtcbiAgICAgIGNvbnN0IHBhcmFtQW5ub3RhdGlvbnMgPSBjdG9yUGFyYW1ldGVycy5tYXAoXG4gICAgICAgICAgKGN0b3JQYXJhbTogYW55KSA9PlxuICAgICAgICAgICAgICBjdG9yUGFyYW0gJiYgY29udmVydFRzaWNrbGVEZWNvcmF0b3JJbnRvTWV0YWRhdGEoY3RvclBhcmFtLmRlY29yYXRvcnMpKTtcbiAgICAgIHJldHVybiB0aGlzLl96aXBUeXBlc0FuZEFubm90YXRpb25zKHBhcmFtVHlwZXMsIHBhcmFtQW5ub3RhdGlvbnMpO1xuICAgIH1cblxuICAgIC8vIEFQSSBmb3IgbWV0YWRhdGEgY3JlYXRlZCBieSBpbnZva2luZyB0aGUgZGVjb3JhdG9ycy5cbiAgICBjb25zdCBwYXJhbUFubm90YXRpb25zID0gdHlwZS5oYXNPd25Qcm9wZXJ0eShQQVJBTUVURVJTKSAmJiAodHlwZSBhcyBhbnkpW1BBUkFNRVRFUlNdO1xuICAgIGNvbnN0IHBhcmFtVHlwZXMgPSB0aGlzLl9yZWZsZWN0ICYmIHRoaXMuX3JlZmxlY3QuZ2V0T3duTWV0YWRhdGEgJiZcbiAgICAgICAgdGhpcy5fcmVmbGVjdC5nZXRPd25NZXRhZGF0YSgnZGVzaWduOnBhcmFtdHlwZXMnLCB0eXBlKTtcbiAgICBpZiAocGFyYW1UeXBlcyB8fCBwYXJhbUFubm90YXRpb25zKSB7XG4gICAgICByZXR1cm4gdGhpcy5femlwVHlwZXNBbmRBbm5vdGF0aW9ucyhwYXJhbVR5cGVzLCBwYXJhbUFubm90YXRpb25zKTtcbiAgICB9XG5cbiAgICAvLyBJZiBhIGNsYXNzIGhhcyBubyBkZWNvcmF0b3JzLCBhdCBsZWFzdCBjcmVhdGUgbWV0YWRhdGFcbiAgICAvLyBiYXNlZCBvbiBmdW5jdGlvbi5sZW5ndGguXG4gICAgLy8gTm90ZTogV2Uga25vdyB0aGF0IHRoaXMgaXMgYSByZWFsIGNvbnN0cnVjdG9yIGFzIHdlIGNoZWNrZWRcbiAgICAvLyB0aGUgY29udGVudCBvZiB0aGUgY29uc3RydWN0b3IgYWJvdmUuXG4gICAgcmV0dXJuIG5ldyBBcnJheSgoPGFueT50eXBlLmxlbmd0aCkpLmZpbGwodW5kZWZpbmVkKTtcbiAgfVxuXG4gIHBhcmFtZXRlcnModHlwZTogVHlwZTxhbnk+KTogYW55W11bXSB7XG4gICAgLy8gTm90ZTogb25seSByZXBvcnQgbWV0YWRhdGEgaWYgd2UgaGF2ZSBhdCBsZWFzdCBvbmUgY2xhc3MgZGVjb3JhdG9yXG4gICAgLy8gdG8gc3RheSBpbiBzeW5jIHdpdGggdGhlIHN0YXRpYyByZWZsZWN0b3IuXG4gICAgaWYgKCFpc1R5cGUodHlwZSkpIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG4gICAgY29uc3QgcGFyZW50Q3RvciA9IGdldFBhcmVudEN0b3IodHlwZSk7XG4gICAgbGV0IHBhcmFtZXRlcnMgPSB0aGlzLl9vd25QYXJhbWV0ZXJzKHR5cGUsIHBhcmVudEN0b3IpO1xuICAgIGlmICghcGFyYW1ldGVycyAmJiBwYXJlbnRDdG9yICE9PSBPYmplY3QpIHtcbiAgICAgIHBhcmFtZXRlcnMgPSB0aGlzLnBhcmFtZXRlcnMocGFyZW50Q3Rvcik7XG4gICAgfVxuICAgIHJldHVybiBwYXJhbWV0ZXJzIHx8IFtdO1xuICB9XG5cbiAgcHJpdmF0ZSBfb3duQW5ub3RhdGlvbnModHlwZU9yRnVuYzogVHlwZTxhbnk+LCBwYXJlbnRDdG9yOiBhbnkpOiBhbnlbXXxudWxsIHtcbiAgICAvLyBQcmVmZXIgdGhlIGRpcmVjdCBBUEkuXG4gICAgaWYgKCg8YW55PnR5cGVPckZ1bmMpLmFubm90YXRpb25zICYmICg8YW55PnR5cGVPckZ1bmMpLmFubm90YXRpb25zICE9PSBwYXJlbnRDdG9yLmFubm90YXRpb25zKSB7XG4gICAgICBsZXQgYW5ub3RhdGlvbnMgPSAoPGFueT50eXBlT3JGdW5jKS5hbm5vdGF0aW9ucztcbiAgICAgIGlmICh0eXBlb2YgYW5ub3RhdGlvbnMgPT09ICdmdW5jdGlvbicgJiYgYW5ub3RhdGlvbnMuYW5ub3RhdGlvbnMpIHtcbiAgICAgICAgYW5ub3RhdGlvbnMgPSBhbm5vdGF0aW9ucy5hbm5vdGF0aW9ucztcbiAgICAgIH1cbiAgICAgIHJldHVybiBhbm5vdGF0aW9ucztcbiAgICB9XG5cbiAgICAvLyBBUEkgb2YgdHNpY2tsZSBmb3IgbG93ZXJpbmcgZGVjb3JhdG9ycyB0byBwcm9wZXJ0aWVzIG9uIHRoZSBjbGFzcy5cbiAgICBpZiAoKDxhbnk+dHlwZU9yRnVuYykuZGVjb3JhdG9ycyAmJiAoPGFueT50eXBlT3JGdW5jKS5kZWNvcmF0b3JzICE9PSBwYXJlbnRDdG9yLmRlY29yYXRvcnMpIHtcbiAgICAgIHJldHVybiBjb252ZXJ0VHNpY2tsZURlY29yYXRvckludG9NZXRhZGF0YSgoPGFueT50eXBlT3JGdW5jKS5kZWNvcmF0b3JzKTtcbiAgICB9XG5cbiAgICAvLyBBUEkgZm9yIG1ldGFkYXRhIGNyZWF0ZWQgYnkgaW52b2tpbmcgdGhlIGRlY29yYXRvcnMuXG4gICAgaWYgKHR5cGVPckZ1bmMuaGFzT3duUHJvcGVydHkoQU5OT1RBVElPTlMpKSB7XG4gICAgICByZXR1cm4gKHR5cGVPckZ1bmMgYXMgYW55KVtBTk5PVEFUSU9OU107XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgYW5ub3RhdGlvbnModHlwZU9yRnVuYzogVHlwZTxhbnk+KTogYW55W10ge1xuICAgIGlmICghaXNUeXBlKHR5cGVPckZ1bmMpKSB7XG4gICAgICByZXR1cm4gW107XG4gICAgfVxuICAgIGNvbnN0IHBhcmVudEN0b3IgPSBnZXRQYXJlbnRDdG9yKHR5cGVPckZ1bmMpO1xuICAgIGNvbnN0IG93bkFubm90YXRpb25zID0gdGhpcy5fb3duQW5ub3RhdGlvbnModHlwZU9yRnVuYywgcGFyZW50Q3RvcikgfHwgW107XG4gICAgY29uc3QgcGFyZW50QW5ub3RhdGlvbnMgPSBwYXJlbnRDdG9yICE9PSBPYmplY3QgPyB0aGlzLmFubm90YXRpb25zKHBhcmVudEN0b3IpIDogW107XG4gICAgcmV0dXJuIHBhcmVudEFubm90YXRpb25zLmNvbmNhdChvd25Bbm5vdGF0aW9ucyk7XG4gIH1cblxuICBwcml2YXRlIF9vd25Qcm9wTWV0YWRhdGEodHlwZU9yRnVuYzogYW55LCBwYXJlbnRDdG9yOiBhbnkpOiB7W2tleTogc3RyaW5nXTogYW55W119fG51bGwge1xuICAgIC8vIFByZWZlciB0aGUgZGlyZWN0IEFQSS5cbiAgICBpZiAoKDxhbnk+dHlwZU9yRnVuYykucHJvcE1ldGFkYXRhICYmXG4gICAgICAgICg8YW55PnR5cGVPckZ1bmMpLnByb3BNZXRhZGF0YSAhPT0gcGFyZW50Q3Rvci5wcm9wTWV0YWRhdGEpIHtcbiAgICAgIGxldCBwcm9wTWV0YWRhdGEgPSAoPGFueT50eXBlT3JGdW5jKS5wcm9wTWV0YWRhdGE7XG4gICAgICBpZiAodHlwZW9mIHByb3BNZXRhZGF0YSA9PT0gJ2Z1bmN0aW9uJyAmJiBwcm9wTWV0YWRhdGEucHJvcE1ldGFkYXRhKSB7XG4gICAgICAgIHByb3BNZXRhZGF0YSA9IHByb3BNZXRhZGF0YS5wcm9wTWV0YWRhdGE7XG4gICAgICB9XG4gICAgICByZXR1cm4gcHJvcE1ldGFkYXRhO1xuICAgIH1cblxuICAgIC8vIEFQSSBvZiB0c2lja2xlIGZvciBsb3dlcmluZyBkZWNvcmF0b3JzIHRvIHByb3BlcnRpZXMgb24gdGhlIGNsYXNzLlxuICAgIGlmICgoPGFueT50eXBlT3JGdW5jKS5wcm9wRGVjb3JhdG9ycyAmJlxuICAgICAgICAoPGFueT50eXBlT3JGdW5jKS5wcm9wRGVjb3JhdG9ycyAhPT0gcGFyZW50Q3Rvci5wcm9wRGVjb3JhdG9ycykge1xuICAgICAgY29uc3QgcHJvcERlY29yYXRvcnMgPSAoPGFueT50eXBlT3JGdW5jKS5wcm9wRGVjb3JhdG9ycztcbiAgICAgIGNvbnN0IHByb3BNZXRhZGF0YSA9IDx7W2tleTogc3RyaW5nXTogYW55W119Pnt9O1xuICAgICAgT2JqZWN0LmtleXMocHJvcERlY29yYXRvcnMpLmZvckVhY2gocHJvcCA9PiB7XG4gICAgICAgIHByb3BNZXRhZGF0YVtwcm9wXSA9IGNvbnZlcnRUc2lja2xlRGVjb3JhdG9ySW50b01ldGFkYXRhKHByb3BEZWNvcmF0b3JzW3Byb3BdKTtcbiAgICAgIH0pO1xuICAgICAgcmV0dXJuIHByb3BNZXRhZGF0YTtcbiAgICB9XG5cbiAgICAvLyBBUEkgZm9yIG1ldGFkYXRhIGNyZWF0ZWQgYnkgaW52b2tpbmcgdGhlIGRlY29yYXRvcnMuXG4gICAgaWYgKHR5cGVPckZ1bmMuaGFzT3duUHJvcGVydHkoUFJPUF9NRVRBREFUQSkpIHtcbiAgICAgIHJldHVybiAodHlwZU9yRnVuYyBhcyBhbnkpW1BST1BfTUVUQURBVEFdO1xuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHByb3BNZXRhZGF0YSh0eXBlT3JGdW5jOiBhbnkpOiB7W2tleTogc3RyaW5nXTogYW55W119IHtcbiAgICBpZiAoIWlzVHlwZSh0eXBlT3JGdW5jKSkge1xuICAgICAgcmV0dXJuIHt9O1xuICAgIH1cbiAgICBjb25zdCBwYXJlbnRDdG9yID0gZ2V0UGFyZW50Q3Rvcih0eXBlT3JGdW5jKTtcbiAgICBjb25zdCBwcm9wTWV0YWRhdGE6IHtba2V5OiBzdHJpbmddOiBhbnlbXX0gPSB7fTtcbiAgICBpZiAocGFyZW50Q3RvciAhPT0gT2JqZWN0KSB7XG4gICAgICBjb25zdCBwYXJlbnRQcm9wTWV0YWRhdGEgPSB0aGlzLnByb3BNZXRhZGF0YShwYXJlbnRDdG9yKTtcbiAgICAgIE9iamVjdC5rZXlzKHBhcmVudFByb3BNZXRhZGF0YSkuZm9yRWFjaCgocHJvcE5hbWUpID0+IHtcbiAgICAgICAgcHJvcE1ldGFkYXRhW3Byb3BOYW1lXSA9IHBhcmVudFByb3BNZXRhZGF0YVtwcm9wTmFtZV07XG4gICAgICB9KTtcbiAgICB9XG4gICAgY29uc3Qgb3duUHJvcE1ldGFkYXRhID0gdGhpcy5fb3duUHJvcE1ldGFkYXRhKHR5cGVPckZ1bmMsIHBhcmVudEN0b3IpO1xuICAgIGlmIChvd25Qcm9wTWV0YWRhdGEpIHtcbiAgICAgIE9iamVjdC5rZXlzKG93blByb3BNZXRhZGF0YSkuZm9yRWFjaCgocHJvcE5hbWUpID0+IHtcbiAgICAgICAgY29uc3QgZGVjb3JhdG9yczogYW55W10gPSBbXTtcbiAgICAgICAgaWYgKHByb3BNZXRhZGF0YS5oYXNPd25Qcm9wZXJ0eShwcm9wTmFtZSkpIHtcbiAgICAgICAgICBkZWNvcmF0b3JzLnB1c2goLi4ucHJvcE1ldGFkYXRhW3Byb3BOYW1lXSk7XG4gICAgICAgIH1cbiAgICAgICAgZGVjb3JhdG9ycy5wdXNoKC4uLm93blByb3BNZXRhZGF0YVtwcm9wTmFtZV0pO1xuICAgICAgICBwcm9wTWV0YWRhdGFbcHJvcE5hbWVdID0gZGVjb3JhdG9ycztcbiAgICAgIH0pO1xuICAgIH1cbiAgICByZXR1cm4gcHJvcE1ldGFkYXRhO1xuICB9XG5cbiAgb3duUHJvcE1ldGFkYXRhKHR5cGVPckZ1bmM6IGFueSk6IHtba2V5OiBzdHJpbmddOiBhbnlbXX0ge1xuICAgIGlmICghaXNUeXBlKHR5cGVPckZ1bmMpKSB7XG4gICAgICByZXR1cm4ge307XG4gICAgfVxuICAgIHJldHVybiB0aGlzLl9vd25Qcm9wTWV0YWRhdGEodHlwZU9yRnVuYywgZ2V0UGFyZW50Q3Rvcih0eXBlT3JGdW5jKSkgfHwge307XG4gIH1cblxuICBoYXNMaWZlY3ljbGVIb29rKHR5cGU6IGFueSwgbGNQcm9wZXJ0eTogc3RyaW5nKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHR5cGUgaW5zdGFuY2VvZiBUeXBlICYmIGxjUHJvcGVydHkgaW4gdHlwZS5wcm90b3R5cGU7XG4gIH1cblxuICBndWFyZHModHlwZTogYW55KToge1trZXk6IHN0cmluZ106IGFueX0geyByZXR1cm4ge307IH1cblxuICBnZXR0ZXIobmFtZTogc3RyaW5nKTogR2V0dGVyRm4geyByZXR1cm4gPEdldHRlckZuPm5ldyBGdW5jdGlvbignbycsICdyZXR1cm4gby4nICsgbmFtZSArICc7Jyk7IH1cblxuICBzZXR0ZXIobmFtZTogc3RyaW5nKTogU2V0dGVyRm4ge1xuICAgIHJldHVybiA8U2V0dGVyRm4+bmV3IEZ1bmN0aW9uKCdvJywgJ3YnLCAncmV0dXJuIG8uJyArIG5hbWUgKyAnID0gdjsnKTtcbiAgfVxuXG4gIG1ldGhvZChuYW1lOiBzdHJpbmcpOiBNZXRob2RGbiB7XG4gICAgY29uc3QgZnVuY3Rpb25Cb2R5ID0gYGlmICghby4ke25hbWV9KSB0aHJvdyBuZXcgRXJyb3IoJ1wiJHtuYW1lfVwiIGlzIHVuZGVmaW5lZCcpO1xuICAgICAgICByZXR1cm4gby4ke25hbWV9LmFwcGx5KG8sIGFyZ3MpO2A7XG4gICAgcmV0dXJuIDxNZXRob2RGbj5uZXcgRnVuY3Rpb24oJ28nLCAnYXJncycsIGZ1bmN0aW9uQm9keSk7XG4gIH1cblxuICAvLyBUaGVyZSBpcyBub3QgYSBjb25jZXB0IG9mIGltcG9ydCB1cmkgaW4gSnMsIGJ1dCB0aGlzIGlzIHVzZWZ1bCBpbiBkZXZlbG9waW5nIERhcnQgYXBwbGljYXRpb25zLlxuICBpbXBvcnRVcmkodHlwZTogYW55KTogc3RyaW5nIHtcbiAgICAvLyBTdGF0aWNTeW1ib2xcbiAgICBpZiAodHlwZW9mIHR5cGUgPT09ICdvYmplY3QnICYmIHR5cGVbJ2ZpbGVQYXRoJ10pIHtcbiAgICAgIHJldHVybiB0eXBlWydmaWxlUGF0aCddO1xuICAgIH1cbiAgICAvLyBSdW50aW1lIHR5cGVcbiAgICByZXR1cm4gYC4vJHtzdHJpbmdpZnkodHlwZSl9YDtcbiAgfVxuXG4gIHJlc291cmNlVXJpKHR5cGU6IGFueSk6IHN0cmluZyB7IHJldHVybiBgLi8ke3N0cmluZ2lmeSh0eXBlKX1gOyB9XG5cbiAgcmVzb2x2ZUlkZW50aWZpZXIobmFtZTogc3RyaW5nLCBtb2R1bGVVcmw6IHN0cmluZywgbWVtYmVyczogc3RyaW5nW10sIHJ1bnRpbWU6IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHJ1bnRpbWU7XG4gIH1cbiAgcmVzb2x2ZUVudW0oZW51bUlkZW50aWZpZXI6IGFueSwgbmFtZTogc3RyaW5nKTogYW55IHsgcmV0dXJuIGVudW1JZGVudGlmaWVyW25hbWVdOyB9XG59XG5cbmZ1bmN0aW9uIGNvbnZlcnRUc2lja2xlRGVjb3JhdG9ySW50b01ldGFkYXRhKGRlY29yYXRvckludm9jYXRpb25zOiBhbnlbXSk6IGFueVtdIHtcbiAgaWYgKCFkZWNvcmF0b3JJbnZvY2F0aW9ucykge1xuICAgIHJldHVybiBbXTtcbiAgfVxuICByZXR1cm4gZGVjb3JhdG9ySW52b2NhdGlvbnMubWFwKGRlY29yYXRvckludm9jYXRpb24gPT4ge1xuICAgIGNvbnN0IGRlY29yYXRvclR5cGUgPSBkZWNvcmF0b3JJbnZvY2F0aW9uLnR5cGU7XG4gICAgY29uc3QgYW5ub3RhdGlvbkNscyA9IGRlY29yYXRvclR5cGUuYW5ub3RhdGlvbkNscztcbiAgICBjb25zdCBhbm5vdGF0aW9uQXJncyA9IGRlY29yYXRvckludm9jYXRpb24uYXJncyA/IGRlY29yYXRvckludm9jYXRpb24uYXJncyA6IFtdO1xuICAgIHJldHVybiBuZXcgYW5ub3RhdGlvbkNscyguLi5hbm5vdGF0aW9uQXJncyk7XG4gIH0pO1xufVxuXG5mdW5jdGlvbiBnZXRQYXJlbnRDdG9yKGN0b3I6IEZ1bmN0aW9uKTogVHlwZTxhbnk+IHtcbiAgY29uc3QgcGFyZW50UHJvdG8gPSBjdG9yLnByb3RvdHlwZSA/IE9iamVjdC5nZXRQcm90b3R5cGVPZihjdG9yLnByb3RvdHlwZSkgOiBudWxsO1xuICBjb25zdCBwYXJlbnRDdG9yID0gcGFyZW50UHJvdG8gPyBwYXJlbnRQcm90by5jb25zdHJ1Y3RvciA6IG51bGw7XG4gIC8vIE5vdGU6IFdlIGFsd2F5cyB1c2UgYE9iamVjdGAgYXMgdGhlIG51bGwgdmFsdWVcbiAgLy8gdG8gc2ltcGxpZnkgY2hlY2tpbmcgbGF0ZXIgb24uXG4gIHJldHVybiBwYXJlbnRDdG9yIHx8IE9iamVjdDtcbn1cbiJdfQ==