package com.database.cli.visitor;

import com.database.Transaction;
import com.database.cli.parser.ASTIdentifier;

import java.io.PrintStream;

class SavepointStatementVisitor extends StatementVisitor {
    public String savepointName;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.savepointName = (String) node.jjtGetValue();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        transaction.savepoint(savepointName);
    }

    @Override
    public StatementType getType() {
        return StatementType.SAVEPOINT;
    }
}