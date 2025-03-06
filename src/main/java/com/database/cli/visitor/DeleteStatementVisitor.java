package com.database.cli.visitor;

import com.database.Transaction;
import com.database.cli.parser.ASTExpression;
import com.database.cli.parser.ASTIdentifier;
import com.database.query.expr.Expression;
import com.database.query.expr.ExpressionVisitor;
import com.database.table.Schema;

import java.io.PrintStream;

class DeleteStatementVisitor extends StatementVisitor {
    public String tableName;
    public Expression cond;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTExpression node, Object data) {
        ExpressionVisitor visitor = new ExpressionVisitor();
        node.jjtAccept(visitor, data);
        this.cond = visitor.build();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            Schema schema = transaction.getSchema(tableName);
            this.cond.setSchema(schema);
            transaction.delete(tableName, cond::evaluate);
            out.println("DELETE");
        } catch (Exception e) {
            out.println(e.getMessage());
            out.println("Failed to execute DELETE.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.DELETE;
    }
}