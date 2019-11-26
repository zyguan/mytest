lexer grammar MyStmt;

SPACE:                               [ \t\r\n]+;
COMMAND_COMMENT:                     '--' [a-zA-Z_]+ [ ]* ~[\r\n]* ('\r'? '\n' | EOF);
SPEC_MYSQL_COMMENT:                  ('/*!' | '/*+') .+? '*/';
BLOCK_COMMENT:                       '/*' .*? '*/' -> channel(HIDDEN);
LINE_COMMENT:                        ('-- ' | '#') ~[\r\n]* ('\r'? '\n' | EOF) -> channel(HIDDEN);

SEMI:                                ';' [ \t\r\n]*;

DQUOTA_STRING:                       '"' ( '\\'. | '""' | ~('"'| '\\') )* '"';
SQUOTA_STRING:                       '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\'';
BQUOTA_STRING:                       '`' ( '\\'. | '``' | ~('`'|'\\'))* '`';

ANY:                                 .;
