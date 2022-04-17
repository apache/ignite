/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { asPureExpressionData } from './types';
import { calcBindingFlags, checkAndUpdateBinding } from './util';
/**
 * @param {?} checkIndex
 * @param {?} argCount
 * @return {?}
 */
export function purePipeDef(checkIndex, argCount) {
    // argCount + 1 to include the pipe as first arg
    return _pureExpressionDef(128 /* TypePurePipe */, checkIndex, new Array(argCount + 1));
}
/**
 * @param {?} checkIndex
 * @param {?} argCount
 * @return {?}
 */
export function pureArrayDef(checkIndex, argCount) {
    return _pureExpressionDef(32 /* TypePureArray */, checkIndex, new Array(argCount));
}
/**
 * @param {?} checkIndex
 * @param {?} propToIndex
 * @return {?}
 */
export function pureObjectDef(checkIndex, propToIndex) {
    /** @type {?} */
    const keys = Object.keys(propToIndex);
    /** @type {?} */
    const nbKeys = keys.length;
    /** @type {?} */
    const propertyNames = new Array(nbKeys);
    for (let i = 0; i < nbKeys; i++) {
        /** @type {?} */
        const key = keys[i];
        /** @type {?} */
        const index = propToIndex[key];
        propertyNames[index] = key;
    }
    return _pureExpressionDef(64 /* TypePureObject */, checkIndex, propertyNames);
}
/**
 * @param {?} flags
 * @param {?} checkIndex
 * @param {?} propertyNames
 * @return {?}
 */
function _pureExpressionDef(flags, checkIndex, propertyNames) {
    /** @type {?} */
    const bindings = new Array(propertyNames.length);
    for (let i = 0; i < propertyNames.length; i++) {
        /** @type {?} */
        const prop = propertyNames[i];
        bindings[i] = {
            flags: 8 /* TypeProperty */,
            name: prop,
            ns: null,
            nonMinifiedName: prop,
            securityContext: null,
            suffix: null
        };
    }
    return {
        // will bet set by the view definition
        nodeIndex: -1,
        parent: null,
        renderParent: null,
        bindingIndex: -1,
        outputIndex: -1,
        // regular values
        checkIndex,
        flags,
        childFlags: 0,
        directChildFlags: 0,
        childMatchedQueries: 0,
        matchedQueries: {},
        matchedQueryIds: 0,
        references: {},
        ngContentIndex: -1,
        childCount: 0, bindings,
        bindingFlags: calcBindingFlags(bindings),
        outputs: [],
        element: null,
        provider: null,
        text: null,
        query: null,
        ngContent: null
    };
}
/**
 * @param {?} view
 * @param {?} def
 * @return {?}
 */
export function createPureExpression(view, def) {
    return { value: undefined };
}
/**
 * @param {?} view
 * @param {?} def
 * @param {?} v0
 * @param {?} v1
 * @param {?} v2
 * @param {?} v3
 * @param {?} v4
 * @param {?} v5
 * @param {?} v6
 * @param {?} v7
 * @param {?} v8
 * @param {?} v9
 * @return {?}
 */
export function checkAndUpdatePureExpressionInline(view, def, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9) {
    /** @type {?} */
    const bindings = def.bindings;
    /** @type {?} */
    let changed = false;
    /** @type {?} */
    const bindLen = bindings.length;
    if (bindLen > 0 && checkAndUpdateBinding(view, def, 0, v0))
        changed = true;
    if (bindLen > 1 && checkAndUpdateBinding(view, def, 1, v1))
        changed = true;
    if (bindLen > 2 && checkAndUpdateBinding(view, def, 2, v2))
        changed = true;
    if (bindLen > 3 && checkAndUpdateBinding(view, def, 3, v3))
        changed = true;
    if (bindLen > 4 && checkAndUpdateBinding(view, def, 4, v4))
        changed = true;
    if (bindLen > 5 && checkAndUpdateBinding(view, def, 5, v5))
        changed = true;
    if (bindLen > 6 && checkAndUpdateBinding(view, def, 6, v6))
        changed = true;
    if (bindLen > 7 && checkAndUpdateBinding(view, def, 7, v7))
        changed = true;
    if (bindLen > 8 && checkAndUpdateBinding(view, def, 8, v8))
        changed = true;
    if (bindLen > 9 && checkAndUpdateBinding(view, def, 9, v9))
        changed = true;
    if (changed) {
        /** @type {?} */
        const data = asPureExpressionData(view, def.nodeIndex);
        /** @type {?} */
        let value;
        switch (def.flags & 201347067 /* Types */) {
            case 32 /* TypePureArray */:
                value = new Array(bindings.length);
                if (bindLen > 0)
                    value[0] = v0;
                if (bindLen > 1)
                    value[1] = v1;
                if (bindLen > 2)
                    value[2] = v2;
                if (bindLen > 3)
                    value[3] = v3;
                if (bindLen > 4)
                    value[4] = v4;
                if (bindLen > 5)
                    value[5] = v5;
                if (bindLen > 6)
                    value[6] = v6;
                if (bindLen > 7)
                    value[7] = v7;
                if (bindLen > 8)
                    value[8] = v8;
                if (bindLen > 9)
                    value[9] = v9;
                break;
            case 64 /* TypePureObject */:
                value = {};
                if (bindLen > 0)
                    value[(/** @type {?} */ (bindings[0].name))] = v0;
                if (bindLen > 1)
                    value[(/** @type {?} */ (bindings[1].name))] = v1;
                if (bindLen > 2)
                    value[(/** @type {?} */ (bindings[2].name))] = v2;
                if (bindLen > 3)
                    value[(/** @type {?} */ (bindings[3].name))] = v3;
                if (bindLen > 4)
                    value[(/** @type {?} */ (bindings[4].name))] = v4;
                if (bindLen > 5)
                    value[(/** @type {?} */ (bindings[5].name))] = v5;
                if (bindLen > 6)
                    value[(/** @type {?} */ (bindings[6].name))] = v6;
                if (bindLen > 7)
                    value[(/** @type {?} */ (bindings[7].name))] = v7;
                if (bindLen > 8)
                    value[(/** @type {?} */ (bindings[8].name))] = v8;
                if (bindLen > 9)
                    value[(/** @type {?} */ (bindings[9].name))] = v9;
                break;
            case 128 /* TypePurePipe */:
                /** @type {?} */
                const pipe = v0;
                switch (bindLen) {
                    case 1:
                        value = pipe.transform(v0);
                        break;
                    case 2:
                        value = pipe.transform(v1);
                        break;
                    case 3:
                        value = pipe.transform(v1, v2);
                        break;
                    case 4:
                        value = pipe.transform(v1, v2, v3);
                        break;
                    case 5:
                        value = pipe.transform(v1, v2, v3, v4);
                        break;
                    case 6:
                        value = pipe.transform(v1, v2, v3, v4, v5);
                        break;
                    case 7:
                        value = pipe.transform(v1, v2, v3, v4, v5, v6);
                        break;
                    case 8:
                        value = pipe.transform(v1, v2, v3, v4, v5, v6, v7);
                        break;
                    case 9:
                        value = pipe.transform(v1, v2, v3, v4, v5, v6, v7, v8);
                        break;
                    case 10:
                        value = pipe.transform(v1, v2, v3, v4, v5, v6, v7, v8, v9);
                        break;
                }
                break;
        }
        data.value = value;
    }
    return changed;
}
/**
 * @param {?} view
 * @param {?} def
 * @param {?} values
 * @return {?}
 */
export function checkAndUpdatePureExpressionDynamic(view, def, values) {
    /** @type {?} */
    const bindings = def.bindings;
    /** @type {?} */
    let changed = false;
    for (let i = 0; i < values.length; i++) {
        // Note: We need to loop over all values, so that
        // the old values are updates as well!
        if (checkAndUpdateBinding(view, def, i, values[i])) {
            changed = true;
        }
    }
    if (changed) {
        /** @type {?} */
        const data = asPureExpressionData(view, def.nodeIndex);
        /** @type {?} */
        let value;
        switch (def.flags & 201347067 /* Types */) {
            case 32 /* TypePureArray */:
                value = values;
                break;
            case 64 /* TypePureObject */:
                value = {};
                for (let i = 0; i < values.length; i++) {
                    value[(/** @type {?} */ (bindings[i].name))] = values[i];
                }
                break;
            case 128 /* TypePurePipe */:
                /** @type {?} */
                const pipe = values[0];
                /** @type {?} */
                const params = values.slice(1);
                value = ((/** @type {?} */ (pipe.transform)))(...params);
                break;
        }
        data.value = value;
    }
    return changed;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHVyZV9leHByZXNzaW9uLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvdmlldy9wdXJlX2V4cHJlc3Npb24udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQTZFLG9CQUFvQixFQUFDLE1BQU0sU0FBUyxDQUFDO0FBQ3pILE9BQU8sRUFBQyxnQkFBZ0IsRUFBRSxxQkFBcUIsRUFBQyxNQUFNLFFBQVEsQ0FBQzs7Ozs7O0FBRS9ELE1BQU0sVUFBVSxXQUFXLENBQUMsVUFBa0IsRUFBRSxRQUFnQjtJQUM5RCxnREFBZ0Q7SUFDaEQsT0FBTyxrQkFBa0IseUJBQXlCLFVBQVUsRUFBRSxJQUFJLEtBQUssQ0FBQyxRQUFRLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQztBQUN6RixDQUFDOzs7Ozs7QUFFRCxNQUFNLFVBQVUsWUFBWSxDQUFDLFVBQWtCLEVBQUUsUUFBZ0I7SUFDL0QsT0FBTyxrQkFBa0IseUJBQTBCLFVBQVUsRUFBRSxJQUFJLEtBQUssQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDO0FBQ3RGLENBQUM7Ozs7OztBQUVELE1BQU0sVUFBVSxhQUFhLENBQUMsVUFBa0IsRUFBRSxXQUFrQzs7VUFDNUUsSUFBSSxHQUFHLE1BQU0sQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDOztVQUMvQixNQUFNLEdBQUcsSUFBSSxDQUFDLE1BQU07O1VBQ3BCLGFBQWEsR0FBRyxJQUFJLEtBQUssQ0FBQyxNQUFNLENBQUM7SUFDdkMsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLE1BQU0sRUFBRSxDQUFDLEVBQUUsRUFBRTs7Y0FDekIsR0FBRyxHQUFHLElBQUksQ0FBQyxDQUFDLENBQUM7O2NBQ2IsS0FBSyxHQUFHLFdBQVcsQ0FBQyxHQUFHLENBQUM7UUFDOUIsYUFBYSxDQUFDLEtBQUssQ0FBQyxHQUFHLEdBQUcsQ0FBQztL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QUNSLEtBQUssQ0FBQzt3QkFDSixLQUFLLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxFQUFFLENBQUMsQ0FBQzt3QkFDM0IsTUFBTTtvQkFDUixLQUFLLENBQUM7d0JBQ0osS0FBSyxHQUFHLElBQUksQ0FBQyxTQUFTLENBQUMsRUFBRSxFQUFFLEVBQUUsQ0FBQyxDQUFDO3dCQUMvQixNQUFNO29CQUNSLEtBQUssQ0FBQzt3QkFDSixLQUFLLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQyxDQUFDO3dCQUNuQyxNQUFNO29CQUNSLEtBQUssQ0FBQzt3QkFDSixLQUFLLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUMsQ0FBQzt3QkFDdkMsTUFBTTtvQkFDUixLQUFLLENBQUM7d0JBQ0osS0FBSyxHQUFHLElBQUksQ0FBQyxTQUFTLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQyxDQUFDO3dCQUMzQyxNQUFNO29CQUNSLEtBQUssQ0FBQzt3QkFDSixLQUFLLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQyxDQUFDO3dCQUMvQyxNQUFNO29CQUNSLEtBQUssQ0FBQzt3QkFDSixLQUFLLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUMsQ0FBQzt3QkFDbkQsTUFBTTtvQkFDUixLQUFLLENBQUM7d0JBQ0osS0FBSyxHQUFHLElBQUksQ0FBQyxTQUFTLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQyxDQUFDO3dCQUN2RCxNQUFNO29CQUNSLEtBQUssRUFBRTt3QkFDTCxLQUFLLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQyxDQUFDO3dCQUMzRCxNQUFNO2lCQUNUO2dCQUNELE1BQU07U0FDVDtRQUNELElBQUksQ0FBQyxLQUFLLEdBQUcsS0FBSyxDQUFDO0tBQ3BCO0lBQ0QsT0FBTyxPQUFPLENBQUM7QUFDakIsQ0FBQzs7Ozs7OztBQUVELE1BQU0sVUFBVSxtQ0FBbUMsQ0FDL0MsSUFBYyxFQUFFLEdBQVksRUFBRSxNQUFhOztVQUN2QyxRQUFRLEdBQUcsR0FBRyxDQUFDLFFBQVE7O1FBQ3pCLE9BQU8sR0FBRyxLQUFLO0lBQ25CLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxNQUFNLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxFQUFFO1FBQ3RDLGlEQUFpRDtRQUNqRCxzQ0FBc0M7UUFDdEMsSUFBSSxxQkFBcUIsQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLENBQUMsRUFBRSxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBRTtZQUNsRCxPQUFPLEdBQUcsSUFBSSxDQUFDO1NBQ2hCO0tBQ0Y7SUFDRCxJQUFJLE9BQU8sRUFBRTs7Y0FDTCxJQUFJLEdBQUcsb0JBQW9CLENBQUMsSUFBSSxFQUFFLEdBQUcsQ0FBQyxTQUFTLENBQUM7O1lBQ2xELEtBQVU7UUFDZCxRQUFRLEdBQUcsQ0FBQyxLQUFLLHdCQUFrQixFQUFFO1lBQ25DO2dCQUNFLEtBQUssR0FBRyxNQUFNLENBQUM7Z0JBQ2YsTUFBTTtZQUNSO2dCQUNFLEtBQUssR0FBRyxFQUFFLENBQUM7Z0JBQ1gsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLE1BQU0sQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLEVBQUU7b0JBQ3RDLEtBQUssQ0FBQyxtQkFBQSxRQUFRLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxFQUFFLENBQUMsR0FBRyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUM7aUJBQ3ZDO2dCQUNELE1BQU07WUFDUjs7c0JBQ1EsSUFBSSxHQUFHLE1BQU0sQ0FBQyxDQUFDLENBQUM7O3NCQUNoQixNQUFNLEdBQUcsTUFBTSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUM7Z0JBQzlCLEtBQUssR0FBRyxDQUFDLG1CQUFLLElBQUksQ0FBQyxTQUFTLEVBQUEsQ0FBQyxDQUFDLEdBQUcsTUFBTSxDQUFDLENBQUM7Z0JBQ3pDLE1BQU07U0FDVDtRQUNELElBQUksQ0FBQyxLQUFLLEdBQUcsS0FBSyxDQUFDO0tBQ3BCO0lBQ0QsT0FBTyxPQUFPLENBQUM7QUFDakIsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtCaW5kaW5nRGVmLCBCaW5kaW5nRmxhZ3MsIE5vZGVEZWYsIE5vZGVGbGFncywgUHVyZUV4cHJlc3Npb25EYXRhLCBWaWV3RGF0YSwgYXNQdXJlRXhwcmVzc2lvbkRhdGF9IGZyb20gJy4vdHlwZXMnO1xuaW1wb3J0IHtjYWxjQmluZGluZ0ZsYWdzLCBjaGVja0FuZFVwZGF0ZUJpbmRpbmd9IGZyb20gJy4vdXRpbCc7XG5cbmV4cG9ydCBmdW5jdGlvbiBwdXJlUGlwZURlZihjaGVja0luZGV4OiBudW1iZXIsIGFyZ0NvdW50OiBudW1iZXIpOiBOb2RlRGVmIHtcbiAgLy8gYXJnQ291bnQgKyAxIHRvIGluY2x1ZGUgdGhlIHBpcGUgYXMgZmlyc3QgYXJnXG4gIHJldHVybiBfcHVyZUV4cHJlc3Npb25EZWYoTm9kZUZsYWdzLlR5cGVQdXJlUGlwZSwgY2hlY2tJbmRleCwgbmV3IEFycmF5KGFyZ0NvdW50ICsgMSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gcHVyZUFycmF5RGVmKGNoZWNrSW5kZXg6IG51bWJlciwgYXJnQ291bnQ6IG51bWJlcik6IE5vZGVEZWYge1xuICByZXR1cm4gX3B1cmVFeHByZXNzaW9uRGVmKE5vZGVGbGFncy5UeXBlUHVyZUFycmF5LCBjaGVja0luZGV4LCBuZXcgQXJyYXkoYXJnQ291bnQpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHB1cmVPYmplY3REZWYoY2hlY2tJbmRleDogbnVtYmVyLCBwcm9wVG9JbmRleDoge1twOiBzdHJpbmddOiBudW1iZXJ9KTogTm9kZURlZiB7XG4gIGNvbnN0IGtleXMgPSBPYmplY3Qua2V5cyhwcm9wVG9JbmRleCk7XG4gIGNvbnN0IG5iS2V5cyA9IGtleXMubGVuZ3RoO1xuICBjb25zdCBwcm9wZXJ0eU5hbWVzID0gbmV3IEFycmF5KG5iS2V5cyk7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgbmJLZXlzOyBpKyspIHtcbiAgICBjb25zdCBrZXkgPSBrZXlzW2ldO1xuICAgIGNvbnN0IGluZGV4ID0gcHJvcFRvSW5kZXhba2V5XTtcbiAgICBwcm9wZXJ0eU5hbWVzW2luZGV4XSA9IGtleTtcbiAgfVxuXG4gIHJldHVybiBfcHVyZUV4cHJlc3Npb25EZWYoTm9kZUZsYWdzLlR5cGVQdXJlT2JqZWN0LCBjaGVja0luZGV4LCBwcm9wZXJ0eU5hbWVzKTtcbn1cblxuZnVuY3Rpb24gX3B1cmVFeHByZXNzaW9uRGVmKFxuICAgIGZsYWdzOiBOb2RlRmxhZ3MsIGNoZWNrSW5kZXg6IG51bWJlciwgcHJvcGVydHlOYW1lczogc3RyaW5nW10pOiBOb2RlRGVmIHtcbiAgY29uc3QgYmluZGluZ3M6IEJpbmRpbmdEZWZbXSA9IG5ldyBBcnJheShwcm9wZXJ0eU5hbWVzLmxlbmd0aCk7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcHJvcGVydHlOYW1lcy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHByb3AgPSBwcm9wZXJ0eU5hbWVzW2ldO1xuICAgIGJpbmRpbmdzW2ldID0ge1xuICAgICAgZmxhZ3M6IEJpbmRpbmdGbGFncy5UeXBlUHJvcGVydHksXG4gICAgICBuYW1lOiBwcm9wLFxuICAgICAgbnM6IG51bGwsXG4gICAgICBub25NaW5pZmllZE5hbWU6IHByb3AsXG4gICAgICBzZWN1cml0eUNvbnRleHQ6IG51bGwsXG4gICAgICBzdWZmaXg6IG51bGxcbiAgICB9O1xuICB9XG4gIHJldHVybiB7XG4gICAgLy8gd2lsbCBiZXQgc2V0IGJ5IHRoZSB2aWV3IGRlZmluaXRpb25cbiAgICBub2RlSW5kZXg6IC0xLFxuICAgIHBhcmVudDogbnVsbCxcbiAgICByZW5kZXJQYXJlbnQ6IG51bGwsXG4gICAgYmluZGluZ0luZGV4OiAtMSxcbiAgICBvdXRwdXRJbmRleDogLTEsXG4gICAgLy8gcmVndWxhciB2YWx1ZXNcbiAgICBjaGVja0luZGV4LFxuICAgIGZsYWdzLFxuICAgIGNoaWxkRmxhZ3M6IDAsXG4gICAgZGlyZWN0Q2hpbGRGbGFnczogMCxcbiAgICBjaGlsZE1hdGNoZWRRdWVyaWVzOiAwLFxuICAgIG1hdGNoZWRRdWVyaWVzOiB7fSxcbiAgICBtYXRjaGVkUXVlcnlJZHM6IDAsXG4gICAgcmVmZXJlbmNlczoge30sXG4gICAgbmdDb250ZW50SW5kZXg6IC0xLFxuICAgIGNoaWxkQ291bnQ6IDAsIGJpbmRpbmdzLFxuICAgIGJpbmRpbmdGbGFnczogY2FsY0JpbmRpbmdGbGFncyhiaW5kaW5ncyksXG4gICAgb3V0cHV0czogW10sXG4gICAgZWxlbWVudDogbnVsbCxcbiAgICBwcm92aWRlcjogbnVsbCxcbiAgICB0ZXh0OiBudWxsLFxuICAgIHF1ZXJ5OiBudWxsLFxuICAgIG5nQ29udGVudDogbnVsbFxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlUHVyZUV4cHJlc3Npb24odmlldzogVmlld0RhdGEsIGRlZjogTm9kZURlZik6IFB1cmVFeHByZXNzaW9uRGF0YSB7XG4gIHJldHVybiB7dmFsdWU6IHVuZGVmaW5lZH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaGVja0FuZFVwZGF0ZVB1cmVFeHByZXNzaW9uSW5saW5lKFxuICAgIHZpZXc6IFZpZXdEYXRhLCBkZWY6IE5vZGVEZWYsIHYwOiBhbnksIHYxOiBhbnksIHYyOiBhbnksIHYzOiBhbnksIHY0OiBhbnksIHY1OiBhbnksIHY2OiBhbnksXG4gICAgdjc6IGFueSwgdjg6IGFueSwgdjk6IGFueSk6IGJvb2xlYW4ge1xuICBjb25zdCBiaW5kaW5ncyA9IGRlZi5iaW5kaW5ncztcbiAgbGV0IGNoYW5nZWQgPSBmYWxzZTtcbiAgY29uc3QgYmluZExlbiA9IGJpbmRpbmdzLmxlbmd0aDtcbiAgaWYgKGJpbmRMZW4gPiAwICYmIGNoZWNrQW5kVXBkYXRlQmluZGluZyh2aWV3LCBkZWYsIDAsIHYwKSkgY2hhbmdlZCA9IHRydWU7XG4gIGlmIChiaW5kTGVuID4gMSAmJiBjaGVja0FuZFVwZGF0ZUJpbmRpbmcodmlldywgZGVmLCAxLCB2MSkpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDIgJiYgY2hlY2tBbmRVcGRhdGVCaW5kaW5nKHZpZXcsIGRlZiwgMiwgdjIpKSBjaGFuZ2VkID0gdHJ1ZTtcbiAgaWYgKGJpbmRMZW4gPiAzICYmIGNoZWNrQW5kVXBkYXRlQmluZGluZyh2aWV3LCBkZWYsIDMsIHYzKSkgY2hhbmdlZCA9IHRydWU7XG4gIGlmIChiaW5kTGVuID4gNCAmJiBjaGVja0FuZFVwZGF0ZUJpbmRpbmcodmlldywgZGVmLCA0LCB2NCkpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDUgJiYgY2hlY2tBbmRVcGRhdGVCaW5kaW5nKHZpZXcsIGRlZiwgNSwgdjUpKSBjaGFuZ2VkID0gdHJ1ZTtcbiAgaWYgKGJpbmRMZW4gPiA2ICYmIGNoZWNrQW5kVXBkYXRlQmluZGluZyh2aWV3LCBkZWYsIDYsIHY2KSkgY2hhbmdlZCA9IHRydWU7XG4gIGlmIChiaW5kTGVuID4gNyAmJiBjaGVja0FuZFVwZGF0ZUJpbmRpbmcodmlldywgZGVmLCA3LCB2NykpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDggJiYgY2hlY2tBbmRVcGRhdGVCaW5kaW5nKHZpZXcsIGRlZiwgOCwgdjgpKSBjaGFuZ2VkID0gdHJ1ZTtcbiAgaWYgKGJpbmRMZW4gPiA5ICYmIGNoZWNrQW5kVXBkYXRlQmluZGluZyh2aWV3LCBkZWYsIDksIHY5KSkgY2hhbmdlZCA9IHRydWU7XG5cbiAgaWYgKGNoYW5nZWQpIHtcbiAgICBjb25zdCBkYXRhID0gYXNQdXJlRXhwcmVzc2lvbkRhdGEodmlldywgZGVmLm5vZGVJbmRleCk7XG4gICAgbGV0IHZhbHVlOiBhbnk7XG4gICAgc3dpdGNoIChkZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuVHlwZXMpIHtcbiAgICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVQdXJlQXJyYXk6XG4gICAgICAgIHZhbHVlID0gbmV3IEFycmF5KGJpbmRpbmdzLmxlbmd0aCk7XG4gICAgICAgIGlmIChiaW5kTGVuID4gMCkgdmFsdWVbMF0gPSB2MDtcbiAgICAgICAgaWYgKGJpbmRMZW4gPiAxKSB2YWx1ZVsxXSA9IHYxO1xuICAgICAgICBpZiAoYmluZExlbiA+IDIpIHZhbHVlWzJdID0gdjI7XG4gICAgICAgIGlmIChiaW5kTGVuID4gMykgdmFsdWVbM10gPSB2MztcbiAgICAgICAgaWYgKGJpbmRMZW4gPiA0KSB2YWx1ZVs0XSA9IHY0O1xuICAgICAgICBpZiAoYmluZExlbiA+IDUpIHZhbHVlWzVdID0gdjU7XG4gICAgICAgIGlmIChiaW5kTGVuID4gNikgdmFsdWVbNl0gPSB2NjtcbiAgICAgICAgaWYgKGJpbmRMZW4gPiA3KSB2YWx1ZVs3XSA9IHY3O1xuICAgICAgICBpZiAoYmluZExlbiA+IDgpIHZhbHVlWzhdID0gdjg7XG4gICAgICAgIGlmIChiaW5kTGVuID4gOSkgdmFsdWVbOV0gPSB2OTtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIE5vZGVGbGFncy5UeXBlUHVyZU9iamVjdDpcbiAgICAgICAgdmFsdWUgPSB7fTtcbiAgICAgICAgaWYgKGJpbmRMZW4gPiAwKSB2YWx1ZVtiaW5kaW5nc1swXS5uYW1lICFdID0gdjA7XG4gICAgICAgIGlmIChiaW5kTGVuID4gMSkgdmFsdWVbYmluZGluZ3NbMV0ubmFtZSAhXSA9IHYxO1xuICAgICAgICBpZiAoYmluZExlbiA+IDIpIHZhbHVlW2JpbmRpbmdzWzJdLm5hbWUgIV0gPSB2MjtcbiAgICAgICAgaWYgKGJpbmRMZW4gPiAzKSB2YWx1ZVtiaW5kaW5nc1szXS5uYW1lICFdID0gdjM7XG4gICAgICAgIGlmIChiaW5kTGVuID4gNCkgdmFsdWVbYmluZGluZ3NbNF0ubmFtZSAhXSA9IHY0O1xuICAgICAgICBpZiAoYmluZExlbiA+IDUpIHZhbHVlW2JpbmRpbmdzWzVdLm5hbWUgIV0gPSB2NTtcbiAgICAgICAgaWYgKGJpbmRMZW4gPiA2KSB2YWx1ZVtiaW5kaW5nc1s2XS5uYW1lICFdID0gdjY7XG4gICAgICAgIGlmIChiaW5kTGVuID4gNykgdmFsdWVbYmluZGluZ3NbN10ubmFtZSAhXSA9IHY3O1xuICAgICAgICBpZiAoYmluZExlbiA+IDgpIHZhbHVlW2JpbmRpbmdzWzhdLm5hbWUgIV0gPSB2ODtcbiAgICAgICAgaWYgKGJpbmRMZW4gPiA5KSB2YWx1ZVtiaW5kaW5nc1s5XS5uYW1lICFdID0gdjk7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBOb2RlRmxhZ3MuVHlwZVB1cmVQaXBlOlxuICAgICAgICBjb25zdCBwaXBlID0gdjA7XG4gICAgICAgIHN3aXRjaCAoYmluZExlbikge1xuICAgICAgICAgIGNhc2UgMTpcbiAgICAgICAgICAgIHZhbHVlID0gcGlwZS50cmFuc2Zvcm0odjApO1xuICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgY2FzZSAyOlxuICAgICAgICAgICAgdmFsdWUgPSBwaXBlLnRyYW5zZm9ybSh2MSk7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgICBjYXNlIDM6XG4gICAgICAgICAgICB2YWx1ZSA9IHBpcGUudHJhbnNmb3JtKHYxLCB2Mik7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgICBjYXNlIDQ6XG4gICAgICAgICAgICB2YWx1ZSA9IHBpcGUudHJhbnNmb3JtKHYxLCB2MiwgdjMpO1xuICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgY2FzZSA1OlxuICAgICAgICAgICAgdmFsdWUgPSBwaXBlLnRyYW5zZm9ybSh2MSwgdjIsIHYzLCB2NCk7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgICBjYXNlIDY6XG4gICAgICAgICAgICB2YWx1ZSA9IHBpcGUudHJhbnNmb3JtKHYxLCB2MiwgdjMsIHY0LCB2NSk7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgICBjYXNlIDc6XG4gICAgICAgICAgICB2YWx1ZSA9IHBpcGUudHJhbnNmb3JtKHYxLCB2MiwgdjMsIHY0LCB2NSwgdjYpO1xuICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgY2FzZSA4OlxuICAgICAgICAgICAgdmFsdWUgPSBwaXBlLnRyYW5zZm9ybSh2MSwgdjIsIHYzLCB2NCwgdjUsIHY2LCB2Nyk7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgICBjYXNlIDk6XG4gICAgICAgICAgICB2YWx1ZSA9IHBpcGUudHJhbnNmb3JtKHYxLCB2MiwgdjMsIHY0LCB2NSwgdjYsIHY3LCB2OCk7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgICBjYXNlIDEwOlxuICAgICAgICAgICAgdmFsdWUgPSBwaXBlLnRyYW5zZm9ybSh2MSwgdjIsIHYzLCB2NCwgdjUsIHY2LCB2NywgdjgsIHY5KTtcbiAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgIH1cbiAgICBkYXRhLnZhbHVlID0gdmFsdWU7XG4gIH1cbiAgcmV0dXJuIGNoYW5nZWQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaGVja0FuZFVwZGF0ZVB1cmVFeHByZXNzaW9uRHluYW1pYyhcbiAgICB2aWV3OiBWaWV3RGF0YSwgZGVmOiBOb2RlRGVmLCB2YWx1ZXM6IGFueVtdKTogYm9vbGVhbiB7XG4gIGNvbnN0IGJpbmRpbmdzID0gZGVmLmJpbmRpbmdzO1xuICBsZXQgY2hhbmdlZCA9IGZhbHNlO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IHZhbHVlcy5sZW5ndGg7IGkrKykge1xuICAgIC8vIE5vdGU6IFdlIG5lZWQgdG8gbG9vcCBvdmVyIGFsbCB2YWx1ZXMsIHNvIHRoYXRcbiAgICAvLyB0aGUgb2xkIHZhbHVlcyBhcmUgdXBkYXRlcyBhcyB3ZWxsIVxuICAgIGlmIChjaGVja0FuZFVwZGF0ZUJpbmRpbmcodmlldywgZGVmLCBpLCB2YWx1ZXNbaV0pKSB7XG4gICAgICBjaGFuZ2VkID0gdHJ1ZTtcbiAgICB9XG4gIH1cbiAgaWYgKGNoYW5nZWQpIHtcbiAgICBjb25zdCBkYXRhID0gYXNQdXJlRXhwcmVzc2lvbkRhdGEodmlldywgZGVmLm5vZGVJbmRleCk7XG4gICAgbGV0IHZhbHVlOiBhbnk7XG4gICAgc3dpdGNoIChkZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuVHlwZXMpIHtcbiAgICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVQdXJlQXJyYXk6XG4gICAgICAgIHZhbHVlID0gdmFsdWVzO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVQdXJlT2JqZWN0OlxuICAgICAgICB2YWx1ZSA9IHt9O1xuICAgICAgICBmb3IgKGxldCBpID0gMDsgaSA8IHZhbHVlcy5sZW5ndGg7IGkrKykge1xuICAgICAgICAgIHZhbHVlW2JpbmRpbmdzW2ldLm5hbWUgIV0gPSB2YWx1ZXNbaV07XG4gICAgICAgIH1cbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIE5vZGVGbGFncy5UeXBlUHVyZVBpcGU6XG4gICAgICAgIGNvbnN0IHBpcGUgPSB2YWx1ZXNbMF07XG4gICAgICAgIGNvbnN0IHBhcmFtcyA9IHZhbHVlcy5zbGljZSgxKTtcbiAgICAgICAgdmFsdWUgPSAoPGFueT5waXBlLnRyYW5zZm9ybSkoLi4ucGFyYW1zKTtcbiAgICAgICAgYnJlYWs7XG4gICAgfVxuICAgIGRhdGEudmFsdWUgPSB2YWx1ZTtcbiAgfVxuICByZXR1cm4gY2hhbmdlZDtcbn1cbiJdfQ==