/* Generated By:JJTree: Do not edit this line. ASTDropIndexStatement.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.database.cli.parser;

public
class ASTDropIndexStatement extends SimpleNode {
  public ASTDropIndexStatement(int id) {
    super(id);
  }

  public ASTDropIndexStatement(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=1dc2c14097baf30a968ee957456a95c3 (do not edit this line) */
