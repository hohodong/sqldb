package com.database.cli.visitor;

import com.database.Transaction;
import com.database.cli.parser.ASTColumnName;
import com.database.cli.parser.ASTExpression;
import com.database.cli.parser.ASTIdentifier;
import com.database.databox.BoolDataBox;
import com.database.query.expr.Expression;
import com.database.query.expr.ExpressionVisitor;
import com.database.table.Schema;

import java.io.PrintStream;

public class UpdateStatementVisitor extends StatementVisitor {
    public String tableName;
    public String updateColumnName;
    public ASTExpression expr;
    public ASTExpression cond;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.updateColumnName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTExpression node, Object data) {
        if (this.expr != null) this.cond = node;
        else this.expr = node;
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            Schema schema = transaction.getSchema(tableName);
            ExpressionVisitor ev = new ExpressionVisitor();
            this.expr.jjtAccept(ev, schema);
            Expression exprFunc = ev.build();
            Expression condFunc;
            if (this.cond == null) {
                condFunc = Expression.literal(new BoolDataBox(true));
            } else {
                ExpressionVisitor condEv = new ExpressionVisitor();
                this.cond.jjtAccept(condEv, schema);
                condFunc = condEv.build();
            }
            exprFunc.setSchema(schema);
            condFunc.setSchema(schema);
            transaction.update(
                    this.tableName,
                    this.updateColumnName,
                    exprFunc::evaluate,
                    condFunc::evaluate
            );
            out.println("UPDATE");
        } catch (Exception e) {
            e.printStackTrace();
            out.println("Failed to execute UPDATE.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.UPDATE;
    }
}
