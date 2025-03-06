#! /usr/bin/bash
rm -rf src/main/java/com/database/cli/parser
jjtree RookieParser.jjt
javacc src/main/java/com/database/cli/parser/RookieParser.jj
rm -f src/main/java/com/database/cli/parser/RookieParser.jj
