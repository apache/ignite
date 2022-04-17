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
/** @enum {number} */
const I18nMutateOpCode = {
    /**
     * Stores shift amount for bits 17-3 that contain reference index.
     */
    SHIFT_REF: 3,
    /**
     * Stores shift amount for bits 31-17 that contain parent index.
     */
    SHIFT_PARENT: 17,
    /**
     * Mask for OpCode
     */
    MASK_OPCODE: 7,
    /**
     * OpCode to select a node. (next OpCode will contain the operation.)
     */
    Select: 0,
    /**
     * OpCode to append the current node to `PARENT`.
     */
    AppendChild: 1,
    /**
     * OpCode to remove the `REF` node from `PARENT`.
     */
    Remove: 3,
    /**
     * OpCode to set the attribute of a node.
     */
    Attr: 4,
    /**
     * OpCode to simulate elementEnd()
     */
    ElementEnd: 5,
    /**
     * OpCode to read the remove OpCodes for the nested ICU
     */
    RemoveNestedIcu: 6,
};
export { I18nMutateOpCode };
/**
 * Marks that the next string is for element.
 *
 * See `I18nMutateOpCodes` documentation.
 * @type {?}
 */
export const ELEMENT_MARKER = {
    marker: 'element'
};
// WARNING: interface has both a type and a value, skipping emit
/**
 * Marks that the next string is for comment.
 *
 * See `I18nMutateOpCodes` documentation.
 * @type {?}
 */
export const COMMENT_MARKER = {
    marker: 'comment'
};
// WARNING: interface has both a type and a value, skipping emit
/**
 * Array storing OpCode for dynamically creating `i18n` blocks.
 *
 * Example:
 * ```ts
 * <I18nCreateOpCode>[
 *   // For adding text nodes
 *   // ---------------------
 *   // Equivalent to:
 *   //   const node = lView[index++] = document.createTextNode('abc');
 *   //   lView[1].insertBefore(node, lView[2]);
 *   'abc', 1 << SHIFT_PARENT | 2 << SHIFT_REF | InsertBefore,
 *
 *   // Equivalent to:
 *   //   const node = lView[index++] = document.createTextNode('xyz');
 *   //   lView[1].appendChild(node);
 *   'xyz', 1 << SHIFT_PARENT | AppendChild,
 *
 *   // For adding element nodes
 *   // ---------------------
 *   // Equivalent to:
 *   //   const node = lView[index++] = document.createElement('div');
 *   //   lView[1].insertBefore(node, lView[2]);
 *   ELEMENT_MARKER, 'div', 1 << SHIFT_PARENT | 2 << SHIFT_REF | InsertBefore,
 *
 *   // Equivalent to:
 *   //   const node = lView[index++] = document.createElement('div');
 *   //   lView[1].appendChild(node);
 *   ELEMENT_MARKER, 'div', 1 << SHIFT_PARENT | AppendChild,
 *
 *   // For adding comment nodes
 *   // ---------------------
 *   // Equivalent to:
 *   //   const node = lView[index++] = document.createComment('');
 *   //   lView[1].insertBefore(node, lView[2]);
 *   COMMENT_MARKER, '', 1 << SHIFT_PARENT | 2 << SHIFT_REF | InsertBefore,
 *
 *   // Equivalent to:
 *   //   const node = lView[index++] = document.createComment('');
 *   //   lView[1].appendChild(node);
 *   COMMENT_MARKER, '', 1 << SHIFT_PARENT | AppendChild,
 *
 *   // For moving existing nodes to a different location
 *   // --------------------------------------------------
 *   // Equivalent to:
 *   //   const node = lView[1];
 *   //   lView[2].insertBefore(node, lView[3]);
 *   1 << SHIFT_REF | Select, 2 << SHIFT_PARENT | 3 << SHIFT_REF | InsertBefore,
 *
 *   // Equivalent to:
 *   //   const node = lView[1];
 *   //   lView[2].appendChild(node);
 *   1 << SHIFT_REF | Select, 2 << SHIFT_PARENT | AppendChild,
 *
 *   // For removing existing nodes
 *   // --------------------------------------------------
 *   //   const node = lView[1];
 *   //   removeChild(tView.data(1), node, lView);
 *   1 << SHIFT_REF | Remove,
 *
 *   // For writing attributes
 *   // --------------------------------------------------
 *   //   const node = lView[1];
 *   //   node.setAttribute('attr', 'value');
 *   1 << SHIFT_REF | Select, 'attr', 'value'
 *            // NOTE: Select followed by two string (vs select followed by OpCode)
 * ];
 * ```
 * NOTE:
 *   - `index` is initial location where the extra nodes should be stored in the EXPANDO section of
 * `LVIewData`.
 *
 * See: `applyI18nCreateOpCodes`;
 * @record
 */
export function I18nMutateOpCodes() { }
/** @enum {number} */
const I18nUpdateOpCode = {
    /**
     * Stores shift amount for bits 17-2 that contain reference index.
     */
    SHIFT_REF: 2,
    /**
     * Mask for OpCode
     */
    MASK_OPCODE: 3,
    /**
     * OpCode to update a text node.
     */
    Text: 0,
    /**
     * OpCode to update a attribute of a node.
     */
    Attr: 1,
    /**
     * OpCode to switch the current ICU case.
     */
    IcuSwitch: 2,
    /**
     * OpCode to update the current ICU case.
     */
    IcuUpdate: 3,
};
export { I18nUpdateOpCode };
/**
 * Stores DOM operations which need to be applied to update DOM render tree due to changes in
 * expressions.
 *
 * The basic idea is that `i18nExp` OpCodes capture expression changes and update a change
 * mask bit. (Bit 1 for expression 1, bit 2 for expression 2 etc..., bit 32 for expression 32 and
 * higher.) The OpCodes then compare its own change mask against the expression change mask to
 * determine if the OpCodes should execute.
 *
 * These OpCodes can be used by both the i18n block as well as ICU sub-block.
 *
 * ## Example
 *
 * Assume
 * ```ts
 *   if (rf & RenderFlags.Update) {
 *    i18nExp(bind(ctx.exp1)); // If changed set mask bit 1
 *    i18nExp(bind(ctx.exp2)); // If changed set mask bit 2
 *    i18nExp(bind(ctx.exp3)); // If changed set mask bit 3
 *    i18nExp(bind(ctx.exp4)); // If changed set mask bit 4
 *    i18nApply(0);            // Apply all changes by executing the OpCodes.
 *  }
 * ```
 * We can assume that each call to `i18nExp` sets an internal `changeMask` bit depending on the
 * index of `i18nExp`.
 *
 * ### OpCodes
 * ```ts
 * <I18nUpdateOpCodes>[
 *   // The following OpCodes represent: `<div i18n-title="pre{{exp1}}in{{exp2}}post">`
 *   // If `changeMask & 0b11`
 *   //        has changed then execute update OpCodes.
 *   //        has NOT changed then skip `7` values and start processing next OpCodes.
 *   0b11, 7,
 *   // Concatenate `newValue = 'pre'+lView[bindIndex-4]+'in'+lView[bindIndex-3]+'post';`.
 *   'pre', -4, 'in', -3, 'post',
 *   // Update attribute: `elementAttribute(1, 'title', sanitizerFn(newValue));`
 *   1 << SHIFT_REF | Attr, 'title', sanitizerFn,
 *
 *   // The following OpCodes represent: `<div i18n>Hello {{exp3}}!">`
 *   // If `changeMask & 0b100`
 *   //        has changed then execute update OpCodes.
 *   //        has NOT changed then skip `4` values and start processing next OpCodes.
 *   0b100, 4,
 *   // Concatenate `newValue = 'Hello ' + lView[bindIndex -2] + '!';`.
 *   'Hello ', -2, '!',
 *   // Update text: `lView[1].textContent = newValue;`
 *   1 << SHIFT_REF | Text,
 *
 *   // The following OpCodes represent: `<div i18n>{exp4, plural, ... }">`
 *   // If `changeMask & 0b1000`
 *   //        has changed then execute update OpCodes.
 *   //        has NOT changed then skip `4` values and start processing next OpCodes.
 *   0b1000, 4,
 *   // Concatenate `newValue = lView[bindIndex -1];`.
 *   -1,
 *   // Switch ICU: `icuSwitchCase(lView[1], 0, newValue);`
 *   0 << SHIFT_ICU | 1 << SHIFT_REF | IcuSwitch,
 *
 *   // Note `changeMask & -1` is always true, so the IcuUpdate will always execute.
 *   -1, 1,
 *   // Update ICU: `icuUpdateCase(lView[1], 0);`
 *   0 << SHIFT_ICU | 1 << SHIFT_REF | IcuUpdate,
 *
 * ];
 * ```
 *
 * @record
 */
export function I18nUpdateOpCodes() { }
/**
 * Store information for the i18n translation block.
 * @record
 */
export function TI18n() { }
if (false) {
    /**
     * Number of slots to allocate in expando.
     *
     * This is the max number of DOM elements which will be created by this i18n + ICU blocks. When
     * the DOM elements are being created they are stored in the EXPANDO, so that update OpCodes can
     * write into them.
     * @type {?}
     */
    TI18n.prototype.vars;
    /**
     * A set of OpCodes which will create the Text Nodes and ICU anchors for the translation blocks.
     *
     * NOTE: The ICU anchors are filled in with ICU Update OpCode.
     * @type {?}
     */
    TI18n.prototype.create;
    /**
     * A set of OpCodes which will be executed on each change detection to determine if any changes to
     * DOM are required.
     * @type {?}
     */
    TI18n.prototype.update;
    /**
     * A list of ICUs in a translation block (or `null` if block has no ICUs).
     *
     * Example:
     * Given: `<div i18n>You have {count, plural, ...} and {state, switch, ...}</div>`
     * There would be 2 ICUs in this array.
     *   1. `{count, plural, ...}`
     *   2. `{state, switch, ...}`
     * @type {?}
     */
    TI18n.prototype.icus;
}
/** @enum {number} */
const IcuType = {
    select: 0,
    plural: 1,
};
export { IcuType };
/**
 * @record
 */
export function TIcu() { }
if (false) {
    /**
     * Defines the ICU type of `select` or `plural`
     * @type {?}
     */
    TIcu.prototype.type;
    /**
     * Number of slots to allocate in expando for each case.
     *
     * This is the max number of DOM elements which will be created by this i18n + ICU blocks. When
     * the DOM elements are being created they are stored in the EXPANDO, so that update OpCodes can
     * write into them.
     * @type {?}
     */
    TIcu.prototype.vars;
    /**
     * An optional array of child/sub ICUs.
     *
     * In case of nested ICUs such as:
     * ```
     * {�0�, plural,
     *   =0 {zero}
     *   other {�0� {�1�, select,
     *                     cat {cats}
     *                     dog {dogs}
     *                     other {animals}
     *                   }!
     *   }
     * }
     * ```
     * When the parent ICU is changing it must clean up child ICUs as well. For this reason it needs
     * to know which child ICUs to run clean up for as well.
     *
     * In the above example this would be:
     * ```ts
     * [
     *   [],   // `=0` has no sub ICUs
     *   [1],  // `other` has one subICU at `1`st index.
     * ]
     * ```
     *
     * The reason why it is Array of Arrays is because first array represents the case, and second
     * represents the child ICUs to clean up. There may be more than one child ICUs per case.
     * @type {?}
     */
    TIcu.prototype.childIcus;
    /**
     * A list of case values which the current ICU will try to match.
     *
     * The last value is `other`
     * @type {?}
     */
    TIcu.prototype.cases;
    /**
     * A set of OpCodes to apply in order to build up the DOM render tree for the ICU
     * @type {?}
     */
    TIcu.prototype.create;
    /**
     * A set of OpCodes to apply in order to destroy the DOM render tree for the ICU.
     * @type {?}
     */
    TIcu.prototype.remove;
    /**
     * A set of OpCodes to apply in order to update the DOM render tree for the ICU bindings.
     * @type {?}
     */
    TIcu.prototype.update;
}
// Note: This hack is necessary so we don't erroneously get a circular dependency
// failure based on types.
/** @type {?} */
export const unusedValueExportToPlacateAjd = 1;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaTE4bi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvaW50ZXJmYWNlcy9pMThuLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7SUFxQkU7O09BRUc7SUFDSCxZQUFhO0lBQ2I7O09BRUc7SUFDSCxnQkFBaUI7SUFDakI7O09BRUc7SUFDSCxjQUFtQjtJQUVuQjs7T0FFRztJQUNILFNBQWM7SUFDZDs7T0FFRztJQUNILGNBQW1CO0lBQ25COztPQUVHO0lBQ0gsU0FBYztJQUNkOztPQUVHO0lBQ0gsT0FBWTtJQUNaOztPQUVHO0lBQ0gsYUFBa0I7SUFDbEI7O09BRUc7SUFDSCxrQkFBdUI7Ozs7Ozs7OztBQVF6QixNQUFNLE9BQU8sY0FBYyxHQUFtQjtJQUM1QyxNQUFNLEVBQUUsU0FBUztDQUNsQjs7Ozs7Ozs7QUFRRCxNQUFNLE9BQU8sY0FBYyxHQUFtQjtJQUM1QyxNQUFNLEVBQUUsU0FBUztDQUNsQjs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUE4RUQsdUNBQ0M7OztJQUdDOztPQUVHO0lBQ0gsWUFBYTtJQUNiOztPQUVHO0lBQ0gsY0FBa0I7SUFFbEI7O09BRUc7SUFDSCxPQUFXO0lBQ1g7O09BRUc7SUFDSCxPQUFXO0lBQ1g7O09BRUc7SUFDSCxZQUFnQjtJQUNoQjs7T0FFRztJQUNILFlBQWdCOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUF1RWxCLHVDQUFtRjs7Ozs7QUFLbkYsMkJBaUNDOzs7Ozs7Ozs7O0lBekJDLHFCQUFhOzs7Ozs7O0lBT2IsdUJBQTBCOzs7Ozs7SUFNMUIsdUJBQTBCOzs7Ozs7Ozs7OztJQVcxQixxQkFBa0I7Ozs7SUFPbEIsU0FBVTtJQUNWLFNBQVU7Ozs7OztBQUdaLDBCQW1FQzs7Ozs7O0lBL0RDLG9CQUFjOzs7Ozs7Ozs7SUFTZCxvQkFBZTs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztJQStCZix5QkFBc0I7Ozs7Ozs7SUFPdEIscUJBQWE7Ozs7O0lBS2Isc0JBQTRCOzs7OztJQUs1QixzQkFBNEI7Ozs7O0lBSzVCLHNCQUE0Qjs7Ozs7QUFLOUIsTUFBTSxPQUFPLDZCQUE2QixHQUFHLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8qKlxuICogYEkxOG5NdXRhdGVPcENvZGVgIGRlZmluZXMgT3BDb2RlcyBmb3IgYEkxOG5NdXRhdGVPcENvZGVzYCBhcnJheS5cbiAqXG4gKiBPcENvZGVzIGNvbnRhaW4gdGhyZWUgcGFydHM6XG4gKiAgMSkgUGFyZW50IG5vZGUgaW5kZXggb2Zmc2V0LlxuICogIDIpIFJlZmVyZW5jZSBub2RlIGluZGV4IG9mZnNldC5cbiAqICAzKSBUaGUgT3BDb2RlIHRvIGV4ZWN1dGUuXG4gKlxuICogU2VlOiBgSTE4bkNyZWF0ZU9wQ29kZXNgIGZvciBleGFtcGxlIG9mIHVzYWdlLlxuICovXG5pbXBvcnQge1Nhbml0aXplckZufSBmcm9tICcuL3Nhbml0aXphdGlvbic7XG5cbmV4cG9ydCBjb25zdCBlbnVtIEkxOG5NdXRhdGVPcENvZGUge1xuICAvKipcbiAgICogU3RvcmVzIHNoaWZ0IGFtb3VudCBmb3IgYml0cyAxNy0zIHRoYXQgY29udGFpbiByZWZlcmVuY2UgaW5kZXguXG4gICAqL1xuICBTSElGVF9SRUYgPSAzLFxuICAvKipcbiAgICogU3RvcmVzIHNoaWZ0IGFtb3VudCBmb3IgYml0cyAzMS0xNyB0aGF0IGNvbnRhaW4gcGFyZW50IGluZGV4LlxuICAgKi9cbiAgU0hJRlRfUEFSRU5UID0gMTcsXG4gIC8qKlxuICAgKiBNYXNrIGZvciBPcENvZGVcbiAgICovXG4gIE1BU0tfT1BDT0RFID0gMGIxMTEsXG5cbiAgLyoqXG4gICAqIE9wQ29kZSB0byBzZWxlY3QgYSBub2RlLiAobmV4dCBPcENvZGUgd2lsbCBjb250YWluIHRoZSBvcGVyYXRpb24uKVxuICAgKi9cbiAgU2VsZWN0ID0gMGIwMDAsXG4gIC8qKlxuICAgKiBPcENvZGUgdG8gYXBwZW5kIHRoZSBjdXJyZW50IG5vZGUgdG8gYFBBUkVOVGAuXG4gICAqL1xuICBBcHBlbmRDaGlsZCA9IDBiMDAxLFxuICAvKipcbiAgICogT3BDb2RlIHRvIHJlbW92ZSB0aGUgYFJFRmAgbm9kZSBmcm9tIGBQQVJFTlRgLlxuICAgKi9cbiAgUmVtb3ZlID0gMGIwMTEsXG4gIC8qKlxuICAgKiBPcENvZGUgdG8gc2V0IHRoZSBhdHRyaWJ1dGUgb2YgYSBub2RlLlxuICAgKi9cbiAgQXR0ciA9IDBiMTAwLFxuICAvKipcbiAgICogT3BDb2RlIHRvIHNpbXVsYXRlIGVsZW1lbnRFbmQoKVxuICAgKi9cbiAgRWxlbWVudEVuZCA9IDBiMTAxLFxuICAvKipcbiAgICogT3BDb2RlIHRvIHJlYWQgdGhlIHJlbW92ZSBPcENvZGVzIGZvciB0aGUgbmVzdGVkIElDVVxuICAgKi9cbiAgUmVtb3ZlTmVzdGVkSWN1ID0gMGIxMTAsXG59XG5cbi8qKlxuICogTWFya3MgdGhhdCB0aGUgbmV4dCBzdHJpbmcgaXMgZm9yIGVsZW1lbnQuXG4gKlxuICogU2VlIGBJMThuTXV0YXRlT3BDb2Rlc2AgZG9jdW1lbnRhdGlvbi5cbiAqL1xuZXhwb3J0IGNvbnN0IEVMRU1FTlRfTUFSS0VSOiBFTEVNRU5UX01BUktFUiA9IHtcbiAgbWFya2VyOiAnZWxlbWVudCdcbn07XG5leHBvcnQgaW50ZXJmYWNlIEVMRU1FTlRfTUFSS0VSIHsgbWFya2VyOiAnZWxlbWVudCc7IH1cblxuLyoqXG4gKiBNYXJrcyB0aGF0IHRoZSBuZXh0IHN0cmluZyBpcyBmb3IgY29tbWVudC5cbiAqXG4gKiBTZWUgYEkxOG5NdXRhdGVPcENvZGVzYCBkb2N1bWVudGF0aW9uLlxuICovXG5leHBvcnQgY29uc3QgQ09NTUVOVF9NQVJLRVI6IENPTU1FTlRfTUFSS0VSID0ge1xuICBtYXJrZXI6ICdjb21tZW50J1xufTtcblxuZXhwb3J0IGludGVyZmFjZSBDT01NRU5UX01BUktFUiB7IG1hcmtlcjogJ2NvbW1lbnQnOyB9XG5cbi8qKlxuICogQXJyYXkgc3RvcmluZyBPcENvZGUgZm9yIGR5bmFtaWNhbGx5IGNyZWF0aW5nIGBpMThuYCBibG9ja3MuXG4gKlxuICogRXhhbXBsZTpcbiAqIGBgYHRzXG4gKiA8STE4bkNyZWF0ZU9wQ29kZT5bXG4gKiAgIC8vIEZvciBhZGRpbmcgdGV4dCBub2Rlc1xuICogICAvLyAtLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqICAgLy8gRXF1aXZhbGVudCB0bzpcbiAqICAgLy8gICBjb25zdCBub2RlID0gbFZpZXdbaW5kZXgrK10gPSBkb2N1bWVudC5jcmVhdGVUZXh0Tm9kZSgnYWJjJyk7XG4gKiAgIC8vICAgbFZpZXdbMV0uaW5zZXJ0QmVmb3JlKG5vZGUsIGxWaWV3WzJdKTtcbiAqICAgJ2FiYycsIDEgPDwgU0hJRlRfUEFSRU5UIHwgMiA8PCBTSElGVF9SRUYgfCBJbnNlcnRCZWZvcmUsXG4gKlxuICogICAvLyBFcXVpdmFsZW50IHRvOlxuICogICAvLyAgIGNvbnN0IG5vZGUgPSBsVmlld1tpbmRleCsrXSA9IGRvY3VtZW50LmNyZWF0ZVRleHROb2RlKCd4eXonKTtcbiAqICAgLy8gICBsVmlld1sxXS5hcHBlbmRDaGlsZChub2RlKTtcbiAqICAgJ3h5eicsIDEgPDwgU0hJRlRfUEFSRU5UIHwgQXBwZW5kQ2hpbGQsXG4gKlxuICogICAvLyBGb3IgYWRkaW5nIGVsZW1lbnQgbm9kZXNcbiAqICAgLy8gLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG4gKiAgIC8vIEVxdWl2YWxlbnQgdG86XG4gKiAgIC8vICAgY29uc3Qgbm9kZSA9IGxWaWV3W2luZGV4KytdID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnZGl2Jyk7XG4gKiAgIC8vICAgbFZpZXdbMV0uaW5zZXJ0QmVmb3JlKG5vZGUsIGxWaWV3WzJdKTtcbiAqICAgRUxFTUVOVF9NQVJLRVIsICdkaXYnLCAxIDw8IFNISUZUX1BBUkVOVCB8IDIgPDwgU0hJRlRfUkVGIHwgSW5zZXJ0QmVmb3JlLFxuICpcbiAqICAgLy8gRXF1aXZhbGVudCB0bzpcbiAqICAgLy8gICBjb25zdCBub2RlID0gbFZpZXdbaW5kZXgrK10gPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KCdkaXYnKTtcbiAqICAgLy8gICBsVmlld1sxXS5hcHBlbmRDaGlsZChub2RlKTtcbiAqICAgRUxFTUVOVF9NQVJLRVIsICdkaXYnLCAxIDw8IFNISUZUX1BBUkVOVCB8IEFwcGVuZENoaWxkLFxuICpcbiAqICAgLy8gRm9yIGFkZGluZyBjb21tZW50IG5vZGVzXG4gKiAgIC8vIC0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuICogICAvLyBFcXVpdmFsZW50IHRvOlxuICogICAvLyAgIGNvbnN0IG5vZGUgPSBsVmlld1tpbmRleCsrXSA9IGRvY3VtZW50LmNyZWF0ZUNvbW1lbnQoJycpO1xuICogICAvLyAgIGxWaWV3WzFdLmluc2VydEJlZm9yZShub2RlLCBsVmlld1syXSk7XG4gKiAgIENPTU1FTlRfTUFSS0VSLCAnJywgMSA8PCBTSElGVF9QQVJFTlQgfCAyIDw8IFNISUZUX1JFRiB8IEluc2VydEJlZm9yZSxcbiAqXG4gKiAgIC8vIEVxdWl2YWxlbnQgdG86XG4gKiAgIC8vICAgY29uc3Qgbm9kZSA9IGxWaWV3W2luZGV4KytdID0gZG9jdW1lbnQuY3JlYXRlQ29tbWVudCgnJyk7XG4gKiAgIC8vICAgbFZpZXdbMV0uYXBwZW5kQ2hpbGQobm9kZSk7XG4gKiAgIENPTU1FTlRfTUFSS0VSLCAnJywgMSA8PCBTSElGVF9QQVJFTlQgfCBBcHBlbmRDaGlsZCxcbiAqXG4gKiAgIC8vIEZvciBtb3ZpbmcgZXhpc3Rpbmcgbm9kZXMgdG8gYSBkaWZmZXJlbnQgbG9jYXRpb25cbiAqICAgLy8gLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqICAgLy8gRXF1aXZhbGVudCB0bzpcbiAqICAgLy8gICBjb25zdCBub2RlID0gbFZpZXdbMV07XG4gKiAgIC8vICAgbFZpZXdbMl0uaW5zZXJ0QmVmb3JlKG5vZGUsIGxWaWV3WzNdKTtcbiAqICAgMSA8PCBTSElGVF9SRUYgfCBTZWxlY3QsIDIgPDwgU0hJRlRfUEFSRU5UIHwgMyA8PCBTSElGVF9SRUYgfCBJbnNlcnRCZWZvcmUsXG4gKlxuICogICAvLyBFcXVpdmFsZW50IHRvOlxuICogICAvLyAgIGNvbnN0IG5vZGUgPSBsVmlld1sxXTtcbiAqICAgLy8gICBsVmlld1syXS5hcHBlbmRDaGlsZChub2RlKTtcbiAqICAgMSA8PCBTSElGVF9SRUYgfCBTZWxlY3QsIDIgPDwgU0hJRlRfUEFSRU5UIHwgQXBwZW5kQ2hpbGQsXG4gKlxuICogICAvLyBGb3IgcmVtb3ZpbmcgZXhpc3Rpbmcgbm9kZXNcbiAqICAgLy8gLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqICAgLy8gICBjb25zdCBub2RlID0gbFZpZXdbMV07XG4gKiAgIC8vICAgcmVtb3ZlQ2hpbGQodFZpZXcuZGF0YSgxKSwgbm9kZSwgbFZpZXcpO1xuICogICAxIDw8IFNISUZUX1JFRiB8IFJlbW92ZSxcbiAqXG4gKiAgIC8vIEZvciB3cml0aW5nIGF0dHJpYnV0ZXNcbiAqICAgLy8gLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqICAgLy8gICBjb25zdCBub2RlID0gbFZpZXdbMV07XG4gKiAgIC8vICAgbm9kZS5zZXRBdHRyaWJ1dGUoJ2F0dHInLCAndmFsdWUnKTtcbiAqICAgMSA8PCBTSElGVF9SRUYgfCBTZWxlY3QsICdhdHRyJywgJ3ZhbHVlJ1xuICogICAgICAgICAgICAvLyBOT1RFOiBTZWxlY3QgZm9sbG93ZWQgYnkgdHdvIHN0cmluZyAodnMgc2VsZWN0IGZvbGxvd2VkIGJ5IE9wQ29kZSlcbiAqIF07XG4gKiBgYGBcbiAqIE5PVEU6XG4gKiAgIC0gYGluZGV4YCBpcyBpbml0aWFsIGxvY2F0aW9uIHdoZXJlIHRoZSBleHRyYSBub2RlcyBzaG91bGQgYmUgc3RvcmVkIGluIHRoZSBFWFBBTkRPIHNlY3Rpb24gb2ZcbiAqIGBMVklld0RhdGFgLlxuICpcbiAqIFNlZTogYGFwcGx5STE4bkNyZWF0ZU9wQ29kZXNgO1xuICovXG5leHBvcnQgaW50ZXJmYWNlIEkxOG5NdXRhdGVPcENvZGVzIGV4dGVuZHMgQXJyYXk8bnVtYmVyfHN0cmluZ3xFTEVNRU5UX01BUktFUnxDT01NRU5UX01BUktFUnxudWxsPiB7XG59XG5cbmV4cG9ydCBjb25zdCBlbnVtIEkxOG5VcGRhdGVPcENvZGUge1xuICAvKipcbiAgICogU3RvcmVzIHNoaWZ0IGFtb3VudCBmb3IgYml0cyAxNy0yIHRoYXQgY29udGFpbiByZWZlcmVuY2UgaW5kZXguXG4gICAqL1xuICBTSElGVF9SRUYgPSAyLFxuICAvKipcbiAgICogTWFzayBmb3IgT3BDb2RlXG4gICAqL1xuICBNQVNLX09QQ09ERSA9IDBiMTEsXG5cbiAgLyoqXG4gICAqIE9wQ29kZSB0byB1cGRhdGUgYSB0ZXh0IG5vZGUuXG4gICAqL1xuICBUZXh0ID0gMGIwMCxcbiAgLyoqXG4gICAqIE9wQ29kZSB0byB1cGRhdGUgYSBhdHRyaWJ1dGUgb2YgYSBub2RlLlxuICAgKi9cbiAgQXR0ciA9IDBiMDEsXG4gIC8qKlxuICAgKiBPcENvZGUgdG8gc3dpdGNoIHRoZSBjdXJyZW50IElDVSBjYXNlLlxuICAgKi9cbiAgSWN1U3dpdGNoID0gMGIxMCxcbiAgLyoqXG4gICAqIE9wQ29kZSB0byB1cGRhdGUgdGhlIGN1cnJlbnQgSUNVIGNhc2UuXG4gICAqL1xuICBJY3VVcGRhdGUgPSAwYjExLFxufVxuXG4vKipcbiAqIFN0b3JlcyBET00gb3BlcmF0aW9ucyB3aGljaCBuZWVkIHRvIGJlIGFwcGxpZWQgdG8gdXBkYXRlIERPTSByZW5kZXIgdHJlZSBkdWUgdG8gY2hhbmdlcyBpblxuICogZXhwcmVzc2lvbnMuXG4gKlxuICogVGhlIGJhc2ljIGlkZWEgaXMgdGhhdCBgaTE4bkV4cGAgT3BDb2RlcyBjYXB0dXJlIGV4cHJlc3Npb24gY2hhbmdlcyBhbmQgdXBkYXRlIGEgY2hhbmdlXG4gKiBtYXNrIGJpdC4gKEJpdCAxIGZvciBleHByZXNzaW9uIDEsIGJpdCAyIGZvciBleHByZXNzaW9uIDIgZXRjLi4uLCBiaXQgMzIgZm9yIGV4cHJlc3Npb24gMzIgYW5kXG4gKiBoaWdoZXIuKSBUaGUgT3BDb2RlcyB0aGVuIGNvbXBhcmUgaXRzIG93biBjaGFuZ2UgbWFzayBhZ2FpbnN0IHRoZSBleHByZXNzaW9uIGNoYW5nZSBtYXNrIHRvXG4gKiBkZXRlcm1pbmUgaWYgdGhlIE9wQ29kZXMgc2hvdWxkIGV4ZWN1dGUuXG4gKlxuICogVGhlc2UgT3BDb2RlcyBjYW4gYmUgdXNlZCBieSBib3RoIHRoZSBpMThuIGJsb2NrIGFzIHdlbGwgYXMgSUNVIHN1Yi1ibG9jay5cbiAqXG4gKiAjIyBFeGFtcGxlXG4gKlxuICogQXNzdW1lXG4gKiBgYGB0c1xuICogICBpZiAocmYgJiBSZW5kZXJGbGFncy5VcGRhdGUpIHtcbiAqICAgIGkxOG5FeHAoYmluZChjdHguZXhwMSkpOyAvLyBJZiBjaGFuZ2VkIHNldCBtYXNrIGJpdCAxXG4gKiAgICBpMThuRXhwKGJpbmQoY3R4LmV4cDIpKTsgLy8gSWYgY2hhbmdlZCBzZXQgbWFzayBiaXQgMlxuICogICAgaTE4bkV4cChiaW5kKGN0eC5leHAzKSk7IC8vIElmIGNoYW5nZWQgc2V0IG1hc2sgYml0IDNcbiAqICAgIGkxOG5FeHAoYmluZChjdHguZXhwNCkpOyAvLyBJZiBjaGFuZ2VkIHNldCBtYXNrIGJpdCA0XG4gKiAgICBpMThuQXBwbHkoMCk7ICAgICAgICAgICAgLy8gQXBwbHkgYWxsIGNoYW5nZXMgYnkgZXhlY3V0aW5nIHRoZSBPcENvZGVzLlxuICogIH1cbiAqIGBgYFxuICogV2UgY2FuIGFzc3VtZSB0aGF0IGVhY2ggY2FsbCB0byBgaTE4bkV4cGAgc2V0cyBhbiBpbnRlcm5hbCBgY2hhbmdlTWFza2AgYml0IGRlcGVuZGluZyBvbiB0aGVcbiAqIGluZGV4IG9mIGBpMThuRXhwYC5cbiAqXG4gKiAjIyMgT3BDb2Rlc1xuICogYGBgdHNcbiAqIDxJMThuVXBkYXRlT3BDb2Rlcz5bXG4gKiAgIC8vIFRoZSBmb2xsb3dpbmcgT3BDb2RlcyByZXByZXNlbnQ6IGA8ZGl2IGkxOG4tdGl0bGU9XCJwcmV7e2V4cDF9fWlue3tleHAyfX1wb3N0XCI+YFxuICogICAvLyBJZiBgY2hhbmdlTWFzayAmIDBiMTFgXG4gKiAgIC8vICAgICAgICBoYXMgY2hhbmdlZCB0aGVuIGV4ZWN1dGUgdXBkYXRlIE9wQ29kZXMuXG4gKiAgIC8vICAgICAgICBoYXMgTk9UIGNoYW5nZWQgdGhlbiBza2lwIGA3YCB2YWx1ZXMgYW5kIHN0YXJ0IHByb2Nlc3NpbmcgbmV4dCBPcENvZGVzLlxuICogICAwYjExLCA3LFxuICogICAvLyBDb25jYXRlbmF0ZSBgbmV3VmFsdWUgPSAncHJlJytsVmlld1tiaW5kSW5kZXgtNF0rJ2luJytsVmlld1tiaW5kSW5kZXgtM10rJ3Bvc3QnO2AuXG4gKiAgICdwcmUnLCAtNCwgJ2luJywgLTMsICdwb3N0JyxcbiAqICAgLy8gVXBkYXRlIGF0dHJpYnV0ZTogYGVsZW1lbnRBdHRyaWJ1dGUoMSwgJ3RpdGxlJywgc2FuaXRpemVyRm4obmV3VmFsdWUpKTtgXG4gKiAgIDEgPDwgU0hJRlRfUkVGIHwgQXR0ciwgJ3RpdGxlJywgc2FuaXRpemVyRm4sXG4gKlxuICogICAvLyBUaGUgZm9sbG93aW5nIE9wQ29kZXMgcmVwcmVzZW50OiBgPGRpdiBpMThuPkhlbGxvIHt7ZXhwM319IVwiPmBcbiAqICAgLy8gSWYgYGNoYW5nZU1hc2sgJiAwYjEwMGBcbiAqICAgLy8gICAgICAgIGhhcyBjaGFuZ2VkIHRoZW4gZXhlY3V0ZSB1cGRhdGUgT3BDb2Rlcy5cbiAqICAgLy8gICAgICAgIGhhcyBOT1QgY2hhbmdlZCB0aGVuIHNraXAgYDRgIHZhbHVlcyBhbmQgc3RhcnQgcHJvY2Vzc2luZyBuZXh0IE9wQ29kZXMuXG4gKiAgIDBiMTAwLCA0LFxuICogICAvLyBDb25jYXRlbmF0ZSBgbmV3VmFsdWUgPSAnSGVsbG8gJyArIGxWaWV3W2JpbmRJbmRleCAtMl0gKyAnISc7YC5cbiAqICAgJ0hlbGxvICcsIC0yLCAnIScsXG4gKiAgIC8vIFVwZGF0ZSB0ZXh0OiBgbFZpZXdbMV0udGV4dENvbnRlbnQgPSBuZXdWYWx1ZTtgXG4gKiAgIDEgPDwgU0hJRlRfUkVGIHwgVGV4dCxcbiAqXG4gKiAgIC8vIFRoZSBmb2xsb3dpbmcgT3BDb2RlcyByZXByZXNlbnQ6IGA8ZGl2IGkxOG4+e2V4cDQsIHBsdXJhbCwgLi4uIH1cIj5gXG4gKiAgIC8vIElmIGBjaGFuZ2VNYXNrICYgMGIxMDAwYFxuICogICAvLyAgICAgICAgaGFzIGNoYW5nZWQgdGhlbiBleGVjdXRlIHVwZGF0ZSBPcENvZGVzLlxuICogICAvLyAgICAgICAgaGFzIE5PVCBjaGFuZ2VkIHRoZW4gc2tpcCBgNGAgdmFsdWVzIGFuZCBzdGFydCBwcm9jZXNzaW5nIG5leHQgT3BDb2Rlcy5cbiAqICAgMGIxMDAwLCA0LFxuICogICAvLyBDb25jYXRlbmF0ZSBgbmV3VmFsdWUgPSBsVmlld1tiaW5kSW5kZXggLTFdO2AuXG4gKiAgIC0xLFxuICogICAvLyBTd2l0Y2ggSUNVOiBgaWN1U3dpdGNoQ2FzZShsVmlld1sxXSwgMCwgbmV3VmFsdWUpO2BcbiAqICAgMCA8PCBTSElGVF9JQ1UgfCAxIDw8IFNISUZUX1JFRiB8IEljdVN3aXRjaCxcbiAqXG4gKiAgIC8vIE5vdGUgYGNoYW5nZU1hc2sgJiAtMWAgaXMgYWx3YXlzIHRydWUsIHNvIHRoZSBJY3VVcGRhdGUgd2lsbCBhbHdheXMgZXhlY3V0ZS5cbiAqICAgLTEsIDEsXG4gKiAgIC8vIFVwZGF0ZSBJQ1U6IGBpY3VVcGRhdGVDYXNlKGxWaWV3WzFdLCAwKTtgXG4gKiAgIDAgPDwgU0hJRlRfSUNVIHwgMSA8PCBTSElGVF9SRUYgfCBJY3VVcGRhdGUsXG4gKlxuICogXTtcbiAqIGBgYFxuICpcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBJMThuVXBkYXRlT3BDb2RlcyBleHRlbmRzIEFycmF5PHN0cmluZ3xudW1iZXJ8U2FuaXRpemVyRm58bnVsbD4ge31cblxuLyoqXG4gKiBTdG9yZSBpbmZvcm1hdGlvbiBmb3IgdGhlIGkxOG4gdHJhbnNsYXRpb24gYmxvY2suXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgVEkxOG4ge1xuICAvKipcbiAgICogTnVtYmVyIG9mIHNsb3RzIHRvIGFsbG9jYXRlIGluIGV4cGFuZG8uXG4gICAqXG4gICAqIFRoaXMgaXMgdGhlIG1heCBudW1iZXIgb2YgRE9NIGVsZW1lbnRzIHdoaWNoIHdpbGwgYmUgY3JlYXRlZCBieSB0aGlzIGkxOG4gKyBJQ1UgYmxvY2tzLiBXaGVuXG4gICAqIHRoZSBET00gZWxlbWVudHMgYXJlIGJlaW5nIGNyZWF0ZWQgdGhleSBhcmUgc3RvcmVkIGluIHRoZSBFWFBBTkRPLCBzbyB0aGF0IHVwZGF0ZSBPcENvZGVzIGNhblxuICAgKiB3cml0ZSBpbnRvIHRoZW0uXG4gICAqL1xuICB2YXJzOiBudW1iZXI7XG5cbiAgLyoqXG4gICAqIEEgc2V0IG9mIE9wQ29kZXMgd2hpY2ggd2lsbCBjcmVhdGUgdGhlIFRleHQgTm9kZXMgYW5kIElDVSBhbmNob3JzIGZvciB0aGUgdHJhbnNsYXRpb24gYmxvY2tzLlxuICAgKlxuICAgKiBOT1RFOiBUaGUgSUNVIGFuY2hvcnMgYXJlIGZpbGxlZCBpbiB3aXRoIElDVSBVcGRhdGUgT3BDb2RlLlxuICAgKi9cbiAgY3JlYXRlOiBJMThuTXV0YXRlT3BDb2RlcztcblxuICAvKipcbiAgICogQSBzZXQgb2YgT3BDb2RlcyB3aGljaCB3aWxsIGJlIGV4ZWN1dGVkIG9uIGVhY2ggY2hhbmdlIGRldGVjdGlvbiB0byBkZXRlcm1pbmUgaWYgYW55IGNoYW5nZXMgdG9cbiAgICogRE9NIGFyZSByZXF1aXJlZC5cbiAgICovXG4gIHVwZGF0ZTogSTE4blVwZGF0ZU9wQ29kZXM7XG5cbiAgLyoqXG4gICAqIEEgbGlzdCBvZiBJQ1VzIGluIGEgdHJhbnNsYXRpb24gYmxvY2sgKG9yIGBudWxsYCBpZiBibG9jayBoYXMgbm8gSUNVcykuXG4gICAqXG4gICAqIEV4YW1wbGU6XG4gICAqIEdpdmVuOiBgPGRpdiBpMThuPllvdSBoYXZlIHtjb3VudCwgcGx1cmFsLCAuLi59IGFuZCB7c3RhdGUsIHN3aXRjaCwgLi4ufTwvZGl2PmBcbiAgICogVGhlcmUgd291bGQgYmUgMiBJQ1VzIGluIHRoaXMgYXJyYXkuXG4gICAqICAgMS4gYHtjb3VudCwgcGx1cmFsLCAuLi59YFxuICAgKiAgIDIuIGB7c3RhdGUsIHN3aXRjaCwgLi4ufWBcbiAgICovXG4gIGljdXM6IFRJY3VbXXxudWxsO1xufVxuXG4vKipcbiAqIERlZmluZXMgdGhlIElDVSB0eXBlIG9mIGBzZWxlY3RgIG9yIGBwbHVyYWxgXG4gKi9cbmV4cG9ydCBjb25zdCBlbnVtIEljdVR5cGUge1xuICBzZWxlY3QgPSAwLFxuICBwbHVyYWwgPSAxLFxufVxuXG5leHBvcnQgaW50ZXJmYWNlIFRJY3Uge1xuICAvKipcbiAgICogRGVmaW5lcyB0aGUgSUNVIHR5cGUgb2YgYHNlbGVjdGAgb3IgYHBsdXJhbGBcbiAgICovXG4gIHR5cGU6IEljdVR5cGU7XG5cbiAgLyoqXG4gICAqIE51bWJlciBvZiBzbG90cyB0byBhbGxvY2F0ZSBpbiBleHBhbmRvIGZvciBlYWNoIGNhc2UuXG4gICAqXG4gICAqIFRoaXMgaXMgdGhlIG1heCBudW1iZXIgb2YgRE9NIGVsZW1lbnRzIHdoaWNoIHdpbGwgYmUgY3JlYXRlZCBieSB0aGlzIGkxOG4gKyBJQ1UgYmxvY2tzLiBXaGVuXG4gICAqIHRoZSBET00gZWxlbWVudHMgYXJlIGJlaW5nIGNyZWF0ZWQgdGhleSBhcmUgc3RvcmVkIGluIHRoZSBFWFBBTkRPLCBzbyB0aGF0IHVwZGF0ZSBPcENvZGVzIGNhblxuICAgKiB3cml0ZSBpbnRvIHRoZW0uXG4gICAqL1xuICB2YXJzOiBudW1iZXJbXTtcblxuICAvKipcbiAgICogQW4gb3B0aW9uYWwgYXJyYXkgb2YgY2hpbGQvc3ViIElDVXMuXG4gICAqXG4gICAqIEluIGNhc2Ugb2YgbmVzdGVkIElDVXMgc3VjaCBhczpcbiAgICogYGBgXG4gICAqIHvvv70w77+9LCBwbHVyYWwsXG4gICAqICAgPTAge3plcm99XG4gICAqICAgb3RoZXIge++/vTDvv70ge++/vTHvv70sIHNlbGVjdCxcbiAgICogICAgICAgICAgICAgICAgICAgICBjYXQge2NhdHN9XG4gICAqICAgICAgICAgICAgICAgICAgICAgZG9nIHtkb2dzfVxuICAgKiAgICAgICAgICAgICAgICAgICAgIG90aGVyIHthbmltYWxzfVxuICAgKiAgICAgICAgICAgICAgICAgICB9IVxuICAgKiAgIH1cbiAgICogfVxuICAgKiBgYGBcbiAgICogV2hlbiB0aGUgcGFyZW50IElDVSBpcyBjaGFuZ2luZyBpdCBtdXN0IGNsZWFuIHVwIGNoaWxkIElDVXMgYXMgd2VsbC4gRm9yIHRoaXMgcmVhc29uIGl0IG5lZWRzXG4gICAqIHRvIGtub3cgd2hpY2ggY2hpbGQgSUNVcyB0byBydW4gY2xlYW4gdXAgZm9yIGFzIHdlbGwuXG4gICAqXG4gICAqIEluIHRoZSBhYm92ZSBleGFtcGxlIHRoaXMgd291bGQgYmU6XG4gICAqIGBgYHRzXG4gICAqIFtcbiAgICogICBbXSwgICAvLyBgPTBgIGhhcyBubyBzdWIgSUNVc1xuICAgKiAgIFsxXSwgIC8vIGBvdGhlcmAgaGFzIG9uZSBzdWJJQ1UgYXQgYDFgc3QgaW5kZXguXG4gICAqIF1cbiAgICogYGBgXG4gICAqXG4gICAqIFRoZSByZWFzb24gd2h5IGl0IGlzIEFycmF5IG9mIEFycmF5cyBpcyBiZWNhdXNlIGZpcnN0IGFycmF5IHJlcHJlc2VudHMgdGhlIGNhc2UsIGFuZCBzZWNvbmRcbiAgICogcmVwcmVzZW50cyB0aGUgY2hpbGQgSUNVcyB0byBjbGVhbiB1cC4gVGhlcmUgbWF5IGJlIG1vcmUgdGhhbiBvbmUgY2hpbGQgSUNVcyBwZXIgY2FzZS5cbiAgICovXG4gIGNoaWxkSWN1czogbnVtYmVyW11bXTtcblxuICAvKipcbiAgICogQSBsaXN0IG9mIGNhc2UgdmFsdWVzIHdoaWNoIHRoZSBjdXJyZW50IElDVSB3aWxsIHRyeSB0byBtYXRjaC5cbiAgICpcbiAgICogVGhlIGxhc3QgdmFsdWUgaXMgYG90aGVyYFxuICAgKi9cbiAgY2FzZXM6IGFueVtdO1xuXG4gIC8qKlxuICAgKiBBIHNldCBvZiBPcENvZGVzIHRvIGFwcGx5IGluIG9yZGVyIHRvIGJ1aWxkIHVwIHRoZSBET00gcmVuZGVyIHRyZWUgZm9yIHRoZSBJQ1VcbiAgICovXG4gIGNyZWF0ZTogSTE4bk11dGF0ZU9wQ29kZXNbXTtcblxuICAvKipcbiAgICogQSBzZXQgb2YgT3BDb2RlcyB0byBhcHBseSBpbiBvcmRlciB0byBkZXN0cm95IHRoZSBET00gcmVuZGVyIHRyZWUgZm9yIHRoZSBJQ1UuXG4gICAqL1xuICByZW1vdmU6IEkxOG5NdXRhdGVPcENvZGVzW107XG5cbiAgLyoqXG4gICAqIEEgc2V0IG9mIE9wQ29kZXMgdG8gYXBwbHkgaW4gb3JkZXIgdG8gdXBkYXRlIHRoZSBET00gcmVuZGVyIHRyZWUgZm9yIHRoZSBJQ1UgYmluZGluZ3MuXG4gICAqL1xuICB1cGRhdGU6IEkxOG5VcGRhdGVPcENvZGVzW107XG59XG5cbi8vIE5vdGU6IFRoaXMgaGFjayBpcyBuZWNlc3Nhcnkgc28gd2UgZG9uJ3QgZXJyb25lb3VzbHkgZ2V0IGEgY2lyY3VsYXIgZGVwZW5kZW5jeVxuLy8gZmFpbHVyZSBiYXNlZCBvbiB0eXBlcy5cbmV4cG9ydCBjb25zdCB1bnVzZWRWYWx1ZUV4cG9ydFRvUGxhY2F0ZUFqZCA9IDE7XG4iXX0=