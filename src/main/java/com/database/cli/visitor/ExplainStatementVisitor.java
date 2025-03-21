package com.database.cli.visitor;

import com.database.Transaction;
import com.database.cli.parser.ASTSelectStatement;
import com.database.query.QueryPlan;

import java.io.PrintStream;

class ExplainStatementVisitor extends StatementVisitor {
    StatementVisitor visitor;

    @Override
    public void visit(ASTSelectStatement node, Object data) {
        SelectStatementVisitor visitor = new SelectStatementVisitor();
        node.jjtAccept(visitor, data);
        this.visitor = visitor;
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        QueryPlan query = this.visitor.getQueryPlan(transaction).get();
        query.execute();
        out.println(query.getFinalOperator());
    }

    @Override
    public StatementType getType() {
        return StatementType.EXPLAIN;
    }

}