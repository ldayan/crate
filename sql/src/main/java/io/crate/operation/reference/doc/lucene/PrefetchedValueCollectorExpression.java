package io.crate.operation.reference.doc.lucene;

import io.crate.types.DataType;

/**
 * A {@link LuceneCollectorExpression} which can be used if a value is already
 * given and has not to be fetched anymore.
 */
public class PrefetchedValueCollectorExpression extends LuceneCollectorExpression<Object> {

    private Object value;

    private final DataType valueType;

    public PrefetchedValueCollectorExpression(DataType valueType) {
        this.valueType = valueType;
    }

    public void value(Object value) {
        this.value = value;
    }

    @Override
    public Object value() {
        return value;
    }

    public DataType valueType() {
        return valueType;
    }
}
