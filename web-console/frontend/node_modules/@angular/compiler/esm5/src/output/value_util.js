/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { visitValue } from '../util';
import * as o from './output_ast';
export var QUOTED_KEYS = '$quoted$';
export function convertValueToOutputAst(ctx, value, type) {
    if (type === void 0) { type = null; }
    return visitValue(value, new _ValueOutputAstTransformer(ctx), type);
}
var _ValueOutputAstTransformer = /** @class */ (function () {
    function _ValueOutputAstTransformer(ctx) {
        this.ctx = ctx;
    }
    _ValueOutputAstTransformer.prototype.visitArray = function (arr, type) {
        var _this = this;
        return o.literalArr(arr.map(function (value) { return visitValue(value, _this, null); }), type);
    };
    _ValueOutputAstTransformer.prototype.visitStringMap = function (map, type) {
        var _this = this;
        var entries = [];
        var quotedSet = new Set(map && map[QUOTED_KEYS]);
        Object.keys(map).forEach(function (key) {
            entries.push(new o.LiteralMapEntry(key, visitValue(map[key], _this, null), quotedSet.has(key)));
        });
        return new o.LiteralMapExpr(entries, type);
    };
    _ValueOutputAstTransformer.prototype.visitPrimitive = function (value, type) { return o.literal(value, type); };
    _ValueOutputAstTransformer.prototype.visitOther = function (value, type) {
        if (value instanceof o.Expression) {
            return value;
        }
        else {
            return this.ctx.importExpr(value);
        }
    };
    return _ValueOutputAstTransformer;
}());
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmFsdWVfdXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9vdXRwdXQvdmFsdWVfdXRpbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFHSCxPQUFPLEVBQWtDLFVBQVUsRUFBQyxNQUFNLFNBQVMsQ0FBQztBQUVwRSxPQUFPLEtBQUssQ0FBQyxNQUFNLGNBQWMsQ0FBQztBQUVsQyxNQUFNLENBQUMsSUFBTSxXQUFXLEdBQUcsVUFBVSxDQUFDO0FBRXRDLE1BQU0sVUFBVSx1QkFBdUIsQ0FDbkMsR0FBa0IsRUFBRSxLQUFVLEVBQUUsSUFBMEI7SUFBMUIscUJBQUEsRUFBQSxXQUEwQjtJQUM1RCxPQUFPLFVBQVUsQ0FBQyxLQUFLLEVBQUUsSUFBSSwwQkFBMEIsQ0FBQyxHQUFHLENBQUMsRUFBRSxJQUFJLENBQUMsQ0FBQztBQUN0RSxDQUFDO0FBRUQ7SUFDRSxvQ0FBb0IsR0FBa0I7UUFBbEIsUUFBRyxHQUFILEdBQUcsQ0FBZTtJQUFHLENBQUM7SUFDMUMsK0NBQVUsR0FBVixVQUFXLEdBQVUsRUFBRSxJQUFZO1FBQW5DLGlCQUVDO1FBREMsT0FBTyxDQUFDLENBQUMsVUFBVSxDQUFDLEdBQUcsQ0FBQyxHQUFHLENBQUMsVUFBQSxLQUFLLElBQUksT0FBQSxVQUFVLENBQUMsS0FBSyxFQUFFLEtBQUksRUFBRSxJQUFJLENBQUMsRUFBN0IsQ0FBNkIsQ0FBQyxFQUFFLElBQUksQ0FBQyxDQUFDO0lBQzdFLENBQUM7SUFFRCxtREFBYyxHQUFkLFVBQWUsR0FBeUIsRUFBRSxJQUFlO1FBQXpELGlCQVFDO1FBUEMsSUFBTSxPQUFPLEdBQXdCLEVBQUUsQ0FBQztRQUN4QyxJQUFNLFNBQVMsR0FBRyxJQUFJLEdBQUcsQ0FBUyxHQUFHLElBQUksR0FBRyxDQUFDLFdBQVcsQ0FBQyxDQUFDLENBQUM7UUFDM0QsTUFBTSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxPQUFPLENBQUMsVUFBQSxHQUFHO1lBQzFCLE9BQU8sQ0FBQyxJQUFJLENBQ1IsSUFBSSxDQUFDLENBQUMsZUFBZSxDQUFDLEdBQUcsRUFBRSxVQUFVLENBQUMsR0FBRyxDQUFDLEdBQUcsQ0FBQyxFQUFFLEtBQUksRUFBRSxJQUFJLENBQUMsRUFBRSxTQUFTLENBQUMsR0FBRyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUN4RixDQUFDLENBQUMsQ0FBQztRQUNILE9BQU8sSUFBSSxDQUFDLENBQUMsY0FBYyxDQUFDLE9BQU8sRUFBRSxJQUFJLENBQUMsQ0FBQztJQUM3QyxDQUFDO0lBRUQsbURBQWMsR0FBZCxVQUFlLEtBQVUsRUFBRSxJQUFZLElBQWtCLE9BQU8sQ0FBQyxDQUFDLE9BQU8sQ0FBQyxLQUFLLEVBQUUsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBRXpGLCtDQUFVLEdBQVYsVUFBVyxLQUFVLEVBQUUsSUFBWTtRQUNqQyxJQUFJLEtBQUssWUFBWSxDQUFDLENBQUMsVUFBVSxFQUFFO1lBQ2pDLE9BQU8sS0FBSyxDQUFDO1NBQ2Q7YUFBTTtZQUNMLE9BQU8sSUFBSSxDQUFDLEdBQUcsQ0FBQyxVQUFVLENBQUMsS0FBSyxDQUFDLENBQUM7U0FDbkM7SUFDSCxDQUFDO0lBQ0gsaUNBQUM7QUFBRCxDQUFDLEFBekJELElBeUJDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5cbmltcG9ydCB7T3V0cHV0Q29udGV4dCwgVmFsdWVUcmFuc2Zvcm1lciwgdmlzaXRWYWx1ZX0gZnJvbSAnLi4vdXRpbCc7XG5cbmltcG9ydCAqIGFzIG8gZnJvbSAnLi9vdXRwdXRfYXN0JztcblxuZXhwb3J0IGNvbnN0IFFVT1RFRF9LRVlTID0gJyRxdW90ZWQkJztcblxuZXhwb3J0IGZ1bmN0aW9uIGNvbnZlcnRWYWx1ZVRvT3V0cHV0QXN0KFxuICAgIGN0eDogT3V0cHV0Q29udGV4dCwgdmFsdWU6IGFueSwgdHlwZTogby5UeXBlIHwgbnVsbCA9IG51bGwpOiBvLkV4cHJlc3Npb24ge1xuICByZXR1cm4gdmlzaXRWYWx1ZSh2YWx1ZSwgbmV3IF9WYWx1ZU91dHB1dEFzdFRyYW5zZm9ybWVyKGN0eCksIHR5cGUpO1xufVxuXG5jbGFzcyBfVmFsdWVPdXRwdXRBc3RUcmFuc2Zvcm1lciBpbXBsZW1lbnRzIFZhbHVlVHJhbnNmb3JtZXIge1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGN0eDogT3V0cHV0Q29udGV4dCkge31cbiAgdmlzaXRBcnJheShhcnI6IGFueVtdLCB0eXBlOiBvLlR5cGUpOiBvLkV4cHJlc3Npb24ge1xuICAgIHJldHVybiBvLmxpdGVyYWxBcnIoYXJyLm1hcCh2YWx1ZSA9PiB2aXNpdFZhbHVlKHZhbHVlLCB0aGlzLCBudWxsKSksIHR5cGUpO1xuICB9XG5cbiAgdmlzaXRTdHJpbmdNYXAobWFwOiB7W2tleTogc3RyaW5nXTogYW55fSwgdHlwZTogby5NYXBUeXBlKTogby5FeHByZXNzaW9uIHtcbiAgICBjb25zdCBlbnRyaWVzOiBvLkxpdGVyYWxNYXBFbnRyeVtdID0gW107XG4gICAgY29uc3QgcXVvdGVkU2V0ID0gbmV3IFNldDxzdHJpbmc+KG1hcCAmJiBtYXBbUVVPVEVEX0tFWVNdKTtcbiAgICBPYmplY3Qua2V5cyhtYXApLmZvckVhY2goa2V5ID0+IHtcbiAgICAgIGVudHJpZXMucHVzaChcbiAgICAgICAgICBuZXcgby5MaXRlcmFsTWFwRW50cnkoa2V5LCB2aXNpdFZhbHVlKG1hcFtrZXldLCB0aGlzLCBudWxsKSwgcXVvdGVkU2V0LmhhcyhrZXkpKSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIG5ldyBvLkxpdGVyYWxNYXBFeHByKGVudHJpZXMsIHR5cGUpO1xuICB9XG5cbiAgdmlzaXRQcmltaXRpdmUodmFsdWU6IGFueSwgdHlwZTogby5UeXBlKTogby5FeHByZXNzaW9uIHsgcmV0dXJuIG8ubGl0ZXJhbCh2YWx1ZSwgdHlwZSk7IH1cblxuICB2aXNpdE90aGVyKHZhbHVlOiBhbnksIHR5cGU6IG8uVHlwZSk6IG8uRXhwcmVzc2lvbiB7XG4gICAgaWYgKHZhbHVlIGluc3RhbmNlb2Ygby5FeHByZXNzaW9uKSB7XG4gICAgICByZXR1cm4gdmFsdWU7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiB0aGlzLmN0eC5pbXBvcnRFeHByKHZhbHVlKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==