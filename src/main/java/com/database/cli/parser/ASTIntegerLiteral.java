/* Generated By:JJTree: Do not edit this line. ASTIntegerLiteral.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.database.cli.parser;

public
class ASTIntegerLiteral extends SimpleNode {
  public ASTIntegerLiteral(int id) {
    super(id);
  }

  public ASTIntegerLiteral(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=119139029fc73359d4f7c1536615de0e (do not edit this line) */
