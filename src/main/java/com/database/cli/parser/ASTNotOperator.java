/* Generated By:JJTree: Do not edit this line. ASTNotOperator.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.database.cli.parser;

public
class ASTNotOperator extends SimpleNode {
  public ASTNotOperator(int id) {
    super(id);
  }

  public ASTNotOperator(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=b1b79d1888c9e948003c7fd991db5663 (do not edit this line) */
