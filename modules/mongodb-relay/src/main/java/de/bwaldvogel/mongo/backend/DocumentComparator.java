package de.bwaldvogel.mongo.backend;

import java.util.Comparator;

import de.bwaldvogel.mongo.bson.Document;

public class DocumentComparator implements Comparator<Document> {

    private Document orderBy;

    public DocumentComparator(Document orderBy) {
        Assert.notNull(orderBy);
        Assert.notEmpty(orderBy.keySet());
        this.orderBy = orderBy;
    }

    @Override
    public int compare(Document document1, Document document2) {
        for (String sortKey : orderBy.keySet()) {
            Object value1 = Utils.getSubdocumentValueCollectionAware(document1, sortKey);
            Object value2 = Utils.getSubdocumentValueCollectionAware(document2, sortKey);

            final ValueComparator comparator;
            if (((Number) orderBy.get(sortKey)).intValue() > 0) {
                comparator = ValueComparator.asc();
            } else {
                comparator = ValueComparator.desc();
            }
            int cmp = comparator.compare(value1, value2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

}
