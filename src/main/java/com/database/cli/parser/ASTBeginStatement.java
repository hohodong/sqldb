/* Generated By:JJTree: Do not edit this line. ASTBeginStatement.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.database.cli.parser;

public
class ASTBeginStatement extends SimpleNode {
  public ASTBeginStatement(int id) {
    super(id);
  }

  public ASTBeginStatement(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=8e1d7627693ff645974a5eb1f1d3073b (do not edit this line) */
