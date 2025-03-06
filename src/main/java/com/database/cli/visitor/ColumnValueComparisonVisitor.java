package com.database.cli.visitor;

import com.database.cli.PrettyPrinter;
import com.database.cli.parser.ASTColumnName;
import com.database.cli.parser.ASTComparisonOperator;
import com.database.cli.parser.ASTLiteral;
import com.database.cli.parser.RookieParserDefaultVisitor;
import com.database.common.PredicateOperator;
import com.database.databox.DataBox;

class ColumnValueComparisonVisitor extends RookieParserDefaultVisitor {
    PredicateOperator op;
    String columnName;
    DataBox value;

    @Override
    public void visit(ASTLiteral node, Object data) {
        this.value = PrettyPrinter.parseLiteral((String) node.jjtGetValue());
    }

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.columnName = (String) node.jjtGetValue();
        // keep things in format columnName <= value
        if (this.op != null) this.op = op.reverse();
    }

    @Override
    public void visit(ASTComparisonOperator node, Object data) {
        this.op = PredicateOperator.fromSymbol((String) node.jjtGetValue());
    }
}
