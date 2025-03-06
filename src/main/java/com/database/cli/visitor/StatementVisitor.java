package com.database.cli.visitor;

import com.database.Transaction;
import com.database.cli.parser.RookieParserDefaultVisitor;
import com.database.query.QueryPlan;

import java.io.PrintStream;
import java.util.Optional;

abstract class StatementVisitor extends RookieParserDefaultVisitor {
    public void execute(Transaction transaction, PrintStream out) {
        throw new UnsupportedOperationException("Statement is not executable.");
    }

    public Optional<String> getSavepointName() {
        return Optional.empty();
    }

    public Optional<QueryPlan> getQueryPlan(Transaction transaction) {
        return Optional.empty();
    }

    public abstract StatementType getType();
}