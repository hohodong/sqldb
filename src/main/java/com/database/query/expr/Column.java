package com.database.query.expr;

import com.database.databox.DataBox;
import com.database.databox.Type;
import com.database.table.Record;
import com.database.table.Schema;

class Column extends Expression {
    private String columnName;
    private Integer col;

    public Column(String columnName) {
        this.columnName = columnName;
        this.dependencies.add(columnName);
        this.col = null;
    }

    @Override
    public void setSchema(Schema schema) {
        super.setSchema(schema);
        this.col = schema.findField(this.columnName);
    }

    @Override
    public Type getType() {
        return schema.getFieldType(this.col);
    }

    @Override
    public DataBox evaluate(Record record) {
        return record.getValue(this.col);
    }

    @Override
    protected OperationPriority priority() {
        return OperationPriority.ATOMIC;
    }

    @Override
    protected String subclassString() {
        return this.columnName;
    }
}

