/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {Observable} from 'rxjs/Observable';
import {merge} from 'rxjs/observable/merge';
import {RejectType} from '@uirouter/angularjs';
import 'rxjs/add/operator/share';
import 'rxjs/add/operator/mapTo';
import 'rxjs/add/operator/startWith';
import isEqual from 'lodash/isEqual';

/**
 * @param {uirouter.TransitionService} $transitions
 */
export default function configSelectionManager($transitions) {
    /**
     * Determines what items should be marked as selected and if something is being edited at the moment.
     */
    return ({itemID$, selectedItemRows$, visibleRows$, loadedItems$}) => {
        // Aborted transitions happen when form has unsaved changes, user attempts to leave
        // but decides to stay after screen asks for leave confirmation.
        const abortedTransitions$ = Observable.create((observer) => {
            return $transitions.onError({}, (t) => observer.next(t));
        })
        .filter((t) => t.error().type === RejectType.ABORTED);

        const firstItemID$ = visibleRows$.withLatestFrom(itemID$, loadedItems$)
            .filter(([rows, id, items]) => !id && rows && rows.length === items.length)
            .pluck('0', '0', 'entity', '_id');

        const selectedItemRowsIDs$ = selectedItemRows$.map((rows) => rows.map((r) => r._id)).share();
        const singleSelectionEdit$ = selectedItemRows$.filter((r) => r && r.length === 1).pluck('0', '_id');
        const selectedMultipleOrNone$ = selectedItemRows$.filter((r) => r.length > 1 || r.length === 0);
        const loadedItemIDs$ = loadedItems$.map((rows) => new Set(rows.map((r) => r._id))).share();
        const currentItemWasRemoved$ = loadedItemIDs$
            .withLatestFrom(
                itemID$.filter((v) => v && v !== 'new'),
                /**
                 * Without startWith currentItemWasRemoved$ won't emit in the following scenario:
                 * 1. User opens items page (no item id in location).
                 * 2. Selection manager commands to edit first item.
                 * 3. User removes said item.
                 */
                selectedItemRowsIDs$.startWith([])
            )
            .filter(([existingIDs, itemID, selectedIDs]) => !existingIDs.has(itemID))
            .map(([existingIDs, itemID, selectedIDs]) => selectedIDs.filter((id) => id !== itemID))
            .share();

        // Edit first loaded item or when there's only one item selected
        const editGoes$ = merge(firstItemID$, singleSelectionEdit$)
            // Don't go to non-existing items.
            // Happens when user naviagtes to older history and some items were already removed.
            .withLatestFrom(loadedItemIDs$).filter(([id, loaded]) => id && loaded.has(id)).pluck('0');
        // Stop edit when multiple or none items are selected or when current item was removed
        const editLeaves$ = merge(
            selectedMultipleOrNone$.mapTo({}),
            currentItemWasRemoved$.mapTo({location: 'replace', custom: {justIDUpdate: true}})
        ).share();

        const selectedItemIDs$ = merge(
            // Select nothing when creating an item or select current item
            itemID$.filter((id) => id).map((id) => id === 'new' ? [] : [id]),
            // Restore previous item selection when transition gets aborted
            abortedTransitions$.withLatestFrom(itemID$, (_, id) => [id]),
            // Select all incoming selected rows
            selectedItemRowsIDs$
        )
        // If nothing's selected and there are zero rows, ui-grid will behave as if all rows are selected
        .startWith([])
        // Some scenarios cause same item to be selected multiple times in a row,
        // so it makes sense to filter out duplicate entries
        .distinctUntilChanged(isEqual)
        .share();

        return {selectedItemIDs$, editGoes$, editLeaves$};
    };
}
configSelectionManager.$inject = ['$transitions'];
