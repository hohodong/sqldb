/* Generated By:JJTree: Do not edit this line. ASTAliasedTableName.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.database.cli.parser;

public
class ASTAliasedTableName extends SimpleNode {
  public ASTAliasedTableName(int id) {
    super(id);
  }

  public ASTAliasedTableName(RookieParser p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=e0276ec514092546c521ac2191708da7 (do not edit this line) */
