/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.HashSet;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * The tree index is an in-memory index based on a binary AVL trees.
 */
public class TreeIndex extends BaseIndex {

    private TreeNode root;
    private final RegularTable tableData;
    private long rowCount;
    private boolean closed;

    public TreeIndex(RegularTable table, int id, String indexName,
            IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
        tableData = table;
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
    }

    @Override
    public void close(Session session) {
        root = null;
        closed = true;
    }

    @Override
    public void add(Session session, Row row) {
        if (closed) {
            throw DbException.throwInternalError();
        }
        TreeNode i = new TreeNode(row);
        TreeNode n = root, x = n;
        boolean isLeft = true;
        while (true) {
            if (n == null) {
                if (x == null) {
                    root = i;
                    rowCount++;
                    return;
                }
                set(x, isLeft, i);
                break;
            }
            Row r = n.row;
            int compare = compareRows(row, r);
            if (compare == 0) {
                if (indexType.isUnique()) {
                    if (!mayHaveNullDuplicates(row)) {
                        throw getDuplicateKeyException(row.toString());
                    }
                }
                compare = compareKeys(row, r);
            }
            isLeft = compare < 0;
            x = n;
            n = child(x, isLeft);
        }
        balance(x, isLeft);
        rowCount++;
    }

    private void balance(TreeNode x, boolean isLeft) {
        while (true) {
            int sign = isLeft ? 1 : -1;
            switch (x.balance * sign) {
            case 1:
                x.balance = 0;
                return;
            case 0:
                x.balance = -sign;
                break;
            case -1:
                TreeNode l = child(x, isLeft);
                if (l.balance == -sign) {
                    replace(x, l);
                    set(x, isLeft, child(l, !isLeft));
                    set(l, !isLeft, x);
                    x.balance = 0;
                    l.balance = 0;
                } else {
                    TreeNode r = child(l, !isLeft);
                    replace(x, r);
                    set(l, !isLeft, child(r, isLeft));
                    set(r, isLeft, l);
                    set(x, isLeft, child(r, !isLeft));
                    set(r, !isLeft, x);
                    int rb = r.balance;
                    x.balance = (rb == -sign) ? sign : 0;
                    l.balance = (rb == sign) ? -sign : 0;
                    r.balance = 0;
                }
                return;
            default:
                DbException.throwInternalError("b:" + x.balance * sign);
            }
            if (x == root) {
                return;
            }
            isLeft = x.isFromLeft();
            x = x.parent;
        }
    }

    private static TreeNode child(TreeNode x, boolean isLeft) {
        return isLeft ? x.left : x.right;
    }

    private void replace(TreeNode x, TreeNode n) {
        if (x == root) {
            root = n;
            if (n != null) {
                n.parent = null;
            }
        } else {
            set(x.parent, x.isFromLeft(), n);
        }
    }

    private static void set(TreeNode parent, boolean left, TreeNode n) {
        if (left) {
            parent.left = n;
        } else {
            parent.right = n;
        }
        if (n != null) {
            n.parent = parent;
        }
    }

    @Override
    public void remove(Session session, Row row) {
        if (closed) {
            throw DbException.throwInternalError();
        }
        TreeNode x = findFirstNode(row, true);
        if (x == null) {
            throw DbException.throwInternalError("not found!");
        }
        TreeNode n;
        if (x.left == null) {
            n = x.right;
        } else if (x.right == null) {
            n = x.left;
        } else {
            TreeNode d = x;
            x = x.left;
            for (TreeNode temp = x; (temp = temp.right) != null;) {
                x = temp;
            }
            // x will be replaced with n later
            n = x.left;
            // swap d and x
            int b = x.balance;
            x.balance = d.balance;
            d.balance = b;

            // set x.parent
            TreeNode xp = x.parent;
            TreeNode dp = d.parent;
            if (d == root) {
                root = x;
            }
            x.parent = dp;
            if (dp != null) {
                if (dp.right == d) {
                    dp.right = x;
                } else {
                    dp.left = x;
                }
            }
            // TODO index / tree: link d.r = x(p?).r directly
            if (xp == d) {
                d.parent = x;
                if (d.left == x) {
                    x.left = d;
                    x.right = d.right;
                } else {
                    x.right = d;
                    x.left = d.left;
                }
            } else {
                d.parent = xp;
                xp.right = d;
                x.right = d.right;
                x.left = d.left;
            }

            if (SysProperties.CHECK && x.right == null) {
                DbException.throwInternalError("tree corrupted");
            }
            x.right.parent = x;
            x.left.parent = x;
            // set d.left, d.right
            d.left = n;
            if (n != null) {
                n.parent = d;
            }
            d.right = null;
            x = d;
        }
        rowCount--;

        boolean isLeft = x.isFromLeft();
        replace(x, n);
        n = x.parent;
        while (n != null) {
            x = n;
            int sign = isLeft ? 1 : -1;
            switch (x.balance * sign) {
            case -1:
                x.balance = 0;
                break;
            case 0:
                x.balance = sign;
                return;
            case 1:
                TreeNode r = child(x, !isLeft);
                int b = r.balance;
                if (b * sign >= 0) {
                    replace(x, r);
                    set(x, !isLeft, child(r, isLeft));
                    set(r, isLeft, x);
                    if (b == 0) {
                        x.balance = sign;
                        r.balance = -sign;
                        return;
                    }
                    x.balance = 0;
                    r.balance = 0;
                    x = r;
                } else {
                    TreeNode l = child(r, isLeft);
                    replace(x, l);
                    b = l.balance;
                    set(r, isLeft, child(l, !isLeft));
                    set(l, !isLeft, r);
                    set(x, !isLeft, child(l, isLeft));
                    set(l, isLeft, x);
                    x.balance = (b == sign) ? -sign : 0;
                    r.balance = (b == -sign) ? sign : 0;
                    l.balance = 0;
                    x = l;
                }
                break;
            default:
                DbException.throwInternalError("b: " + x.balance * sign);
            }
            isLeft = x.isFromLeft();
            n = x.parent;
        }
    }

    private TreeNode findFirstNode(SearchRow row, boolean withKey) {
        TreeNode x = root, result = x;
        while (x != null) {
            result = x;
            int compare = compareRows(x.row, row);
            if (compare == 0 && withKey) {
                compare = compareKeys(x.row, row);
            }
            if (compare == 0) {
                if (withKey) {
                    return x;
                }
                x = x.left;
            } else if (compare > 0) {
                x = x.left;
            } else {
                x = x.right;
            }
        }
        return result;
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return find(first, last);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(first, last);
    }

    private Cursor find(SearchRow first, SearchRow last) {
        if (first == null) {
            TreeNode x = root, n;
            while (x != null) {
                n = x.left;
                if (n == null) {
                    break;
                }
                x = n;
            }
            return new TreeCursor(this, x, null, last);
        }
        TreeNode x = findFirstNode(first, false);
        return new TreeCursor(this, x, first, last);
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter[] filters, int filter,
            SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        return getCostRangeIndex(masks, tableData.getRowCountApproximation(),
                filters, filter, sortOrder, false, allColumnsSet);
    }

    @Override
    public void remove(Session session) {
        truncate(session);
    }

    @Override
    public void truncate(Session session) {
        root = null;
        rowCount = 0;
    }

    @Override
    public void checkRename() {
        // nothing to do
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        if (closed) {
            throw DbException.throwInternalError(toString());
        }
        if (first) {
            // TODO optimization: this loops through NULL
            Cursor cursor = find(session, null, null);
            while (cursor.next()) {
                SearchRow row = cursor.getSearchRow();
                Value v = row.getValue(columnIds[0]);
                if (v != ValueNull.INSTANCE) {
                    return cursor;
                }
            }
            return cursor;
        }
        TreeNode x = root, n;
        while (x != null) {
            n = x.right;
            if (n == null) {
                break;
            }
            x = n;
        }
        TreeCursor cursor = new TreeCursor(this, x, null, null);
        if (x == null) {
            return cursor;
        }
        // TODO optimization: this loops through NULL elements
        do {
            SearchRow row = cursor.getSearchRow();
            if (row == null) {
                break;
            }
            Value v = row.getValue(columnIds[0]);
            if (v != ValueNull.INSTANCE) {
                return cursor;
            }
        } while (cursor.previous());
        return cursor;
    }

    @Override
    public long getRowCount(Session session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

}
