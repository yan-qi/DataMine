grammar PQL;

@header {
package datamine.query.parser;
}

pql_clause
    : query_clause
    ;

query_clause
    : (select_statement)+
    ;

select_statement :
    SELECT select_list
    FROM table_list
    WHEN time_window
    (WHERE where=search_condition)?
    (HAVING having=search_condition)?
    (ORDER BY order_list)?
//    (LIMIT DECIMAL)?
//    (SAMPLE sampling_constraint)?
    ;

order_list
    : order_by_expression (',' order_by_expression)*;

order_by_expression
    : expression (ASC | DESC)?
    ;

time_window
    : '[' date_expression ',' date_expression ']' (TIME_ZONE time_zone)?
    | '[' DECIMAL ',' DECIMAL ']' (TIME_ZONE time_zone)?
    | PAST DECIMAL DAYS (TIME_ZONE time_zone)?
    ;

date_expression
    : DATE (sign DECIMAL)?
    ;

time_zone
    : sign DECIMAL                                      #time_zone_hr
    | ID                                                #time_zone_id
    | expression                                        #time_zone_ex
    ;


table_list
    : table_name (',' table_name)*
    ;

select_list
    : select_list_elem (',' select_list_elem)*
    ;

select_list_elem
    : expression (AS? column_alias)?
    ;


expression
    : NULL                                                     #primitive_expression
    | constant                                                 #primitive_expression
    | function_call                                            #function_call_expression
    | CASE switch_search_condition_section+ (ELSE elseExpr=expression)? END      #case_expression
    | full_column_name                                         #column_ref_expression
    | '(' expression ')'                                       #bracket_expression
    | '~' expression                                           #not_operator_expression
    | expression op=('*' | '/' | '%') expression               #binary_operator_expression
    | op=('+' | '-') expression                                #posneg_operator_expression
    | expression op=('+' | '-' | '&' | '^' | '|') expression   #binary_operator_expression
    | expression comparison_operator expression                #binary_boolean_operator_expression
    ;

function_call
    : aggregate_windowed_function                               #aggregateWindowedFunction
    | nesting_windowed_function                                 #nestingWindowedFunction
    | BIN '(' start=expression ',' duration=expression ',' size=expression (',' max=expression ',' min=expression)?')'                         #binFunction
    | CAST '(' expression AS data_type ')'                      #castFunction
    | CASTLONG '(' expression ')'     #castLongFunction
    | CONVERT '(' data_type ',' expression (',' style=expression)? ')' # convertFunction
    | COALESCE '(' expression_list ')'                          #coalesceFunction
    | CURRENT_TIMESTAMP                                         #currentTimestampFunction
    | DATEADD '(' ID ',' expression ',' expression ')'          #dateAddFunction
    | DATEDIFF '(' ID ',' expression ',' expression ')'         #dateDiffFunction
    | date_conversion                                           #dateConversion
    | TO_TIMESTAMP '(' expression? (',' time_zone)? (',' pattern=STRING)? ')' # timestampConversion
    | GETUTCDATE '(' ')'                                        #getUTCDateFunction
    | HASH '(' expression ')'                                   #hashFunction
    | IDENTITY '(' data_type (',' seed=DECIMAL)? (',' increment=DECIMAL)? ')'   #identityFunction
    | NULLIF '(' expression ',' expression ')'                  #nullIfFunction
    | CONCAT '(' expression_list ')'                            #concatFunction
    | EXTSTR '(' expression ',' pattern=STRING (',' index=DECIMAL)? ')' #extractStringFunction
    | JSON_VAL '(' expression ',' expression ')'                #getFromJsonFunction
    | SUBSTR '(' expression ',' expression (',' end=expression)? ')'#subStringFunction
    ;

// TODO Implement according to the application
data_type
    /*: BIGINT
    | BINARY '(' DECIMAL ')'
    | BIT
    | CHAR '(' DECIMAL ')'
    | DATE
    | DATETIME
    | DATETIME2
    | DATETIMEOFFSET '(' DECIMAL ')'
    | DECIMAL '(' DECIMAL ',' DECIMAL ')'
    | FLOAT
    | GEOGRAPHY
    | GEOMETRY
    | HIERARCHYID
    | IMAGE
    | INT
    | MONEY
    | NCHAR '(' DECIMAL ')'
    | NTEXT
    | NUMERIC '(' DECIMAL ',' DECIMAL ')'
    | NVARCHAR '(' DECIMAL | MAX ')'
    | REAL
    | SMALLDATETIME
    | SMALLINT
    | SMALLMONEY
    | SQL_VARIANT
    | TEXT
    | TIME '(' DECIMAL ')'
    | TIMESTAMP
    | TINYINT
    | UNIQUEIDENTIFIER
    | VARBINARY '(' DECIMAL | MAX ')'
    | VARCHAR '(' DECIMAL | MAX ')'
    | XML*/
    : ID IDENTITY? ('(' (DECIMAL | MAX) (',' DECIMAL)? ')')?
    ;

date_conversion
    : opr=(MONTHOF | WEEKINYEAR | DAYINWEEK | DATEOF | HOUROF | YEAROF )
    '(' expression (',' time_zone)? (',' pattern=STRING)? ')'
    ;

nf_full_column_name
    : full_column_name
    ;

nesting_func_search_predicate
    : expression comparison_operator expression             #nf_comparison_expression
    | expression NOT? BETWEEN expression AND expression     #nf_between_expression
    | expression NOT? IN '(' expression_list ')'            #nf_in_expression
    | expression NOT? LIKE expression                       #nf_like_expression
    ;

nesting_windowed_function
    : (NF_MAX | NF_MIN | NF_SUM)
      '(' nf_full_column_name (',' search_condition)? (',' LIMIT DECIMAL) ? ')' #nfStandardFuncion
    | NF_COUNT '(' (ALL | DISTINCT)? nf_full_column_name (',' search_condition)? (',' LIMIT DECIMAL) ? ')' #nfCountFunction
    | PLAY_SESSION_COUNT '('nf_full_column_name ',' (START_TIME_IN_MS)? nf_full_column_name ',' (DURATION_TIME_IN_SEC)? nf_full_column_name  (',' MIN_DURATION_IN_MS DECIMAL)? (',' TIMEOUT DECIMAL)?  (',' search_condition)? (',' LIMIT DECIMAL) ? ')'    #playSessionCountFunction
    ;



aggregate_windowed_function
    : (AVG | MAX | MIN | SUM | STDEV | STDEVP | VAR | VARP)
      '(' all_distinct_expression ')'                       #standardAggregateWindowFunction
    | COUNT
      '(' ('*' | distinct_expression) ')'                   #countAggregateWindowFunction
    | HIST_COUNT '(' expression (',' DISTINCT expression)?')'  #histCountAggregateWindowFunction
    | HIST_COUNT2 '(' expression (',' DISTINCT expression)? (',' MIN_FREQ DECIMAL) ? ')'  #histCountAggregateWindowFunction2
    | GROUPING '(' expression ')'                           #groupAggregateWindowFunction
    | GROUPING_ID '(' expression_list ')'                   #groupIdAggregateWindowFunction
//    | CONCAT '(' expression_list ')'                        #concatAggregateWindowFunction
    ;

all_distinct_expression
    : (ALL | DISTINCT)? expression
    ;

distinct_expression
    : DISTINCT expression
    ;


switch_search_condition_section
    : WHEN search_condition THEN expression
    ;


search_condition
    : search_condition_and (OR search_condition_and)*
    ;

nesting_search_condition
    : search_condition_and (OR search_condition_and)*
    ;

search_condition_and
    : search_condition_not (AND search_condition_not)*
    ;

search_condition_not
    : NOT? predicate
    ;

expression_list
    : expression (',' expression)*
    ;

predicate
    : expression comparison_operator expression             #comparison_expression
    | expression NOT? BETWEEN expression AND expression     #between_expression
    | expression NOT? IN '(' expression_list ')'            #in_expression
//    | expression NOT? LIKE expression (ESCAPE expression)?  #like_expression
    | expression NOT? LIKE expression                       #like_expression
    | expression IS null_notnull                            #is_null_or_not_null
    | '(' nesting_search_condition ')'                      #nested_search_condition
    ;

null_notnull
    : NOT? NULL
    ;

full_column_name:
    (table_name '.')? ID
    ;

table_name
    : (ID '.')* ID ('(' full_column_name ')')?
    ;

constant
    : STRING // string, datetime or uniqueidentifier
    | BINARY
    | sign? DECIMAL
    | sign? (REAL | FLOAT)  // float or decimal
    | sign? dollar='$' (DECIMAL | FLOAT)       // money
    ;

sign
    : '+'
    | '-'
    ;

column_alias
    : ID
    | STRING
    ;

comparison_operator
    : '=' | '>' | '<' | '<' '=' | '>' '=' | '<' '>' | '!' '=' | '!' '>' | '!' '<'
    ;

//id:
//    simple_id
//    | DOUBLE_QUOTE_ID
//    | SQUARE_BRACKET_ID
//    ;

TIME_ZONE       : T I M E '_' Z O N E | T Z;
DATE            : DEC_DIGIT DEC_DIGIT DEC_DIGIT DEC_DIGIT '/' DEC_DIGIT DEC_DIGIT '/' DEC_DIGIT DEC_DIGIT;


ADD:                                   A D D;
ALL:                                   A L L;
ALTER:                                 A L T E R;
AND:                                   A N D | '&' '&';
ANY:                                   A N Y;
APPEND:                                A P P E N D;
AS:                                    A S;
ASC:                                   A S C;
ASYMMETRIC:                            A S Y M M E T R I C;
AUTHORIZATION:                         A U T H O R I Z A T I O N;
BACKUP:                                B A C K U P;
BEGIN:                                 B E G I N;
BETWEEN:                               B E T W E E N;
BIN:                                   B I N;
BREAK:                                 B R E A K;
BROWSE:                                B R O W S E;
BULK:                                  B U L K;
BY:                                    B Y;
CALLED:                                C A L L E D;
CASCADE:                               C A S C A D E;
CASE:                                  C A S E;
CERTIFICATE:                           C E R T I F I C A T E;
CHANGETABLE:                           C H A N G E T A B L E;
CHANGES:                               C H A N G E S;
CHECK:                                 C H E C K;
CHECKPOINT:                            C H E C K P O I N T;
CLOSE:                                 C L O S E;
CLUSTERED:                             C L U S T E R E D;
COALESCE:                              C O A L E S C E;
COLLATE:                               C O L L A T E;
COLUMN:                                C O L U M N;
COMMIT:                                C O M M I T;
COMPUTE:                               C O M P U T E;
CONSTRAINT:                            C O N S T R A I N T;
CONTAINMENT:                           C O N T A I N M E N T;
CONTAINS:                              C O N T A I N S;
CONTAINSTABLE:                         C O N T A I N S T A B L E;
CONTINUE:                              C O N T I N U E;
CONTRACT:                              C O N T R A C T;
CONVERSATION:                          C O N V E R S A T I O N;
CONVERT:                               (T R Y '_')? C O N V E R T;
CREATE:                                C R E A T E;
CROSS:                                 C R O S S;
CURRENT:                               C U R R E N T;
CURRENT_DATE:                          C U R R E N T '_' D A T E;
CURRENT_TIME:                          C U R R E N T '_' T I M E;
CURRENT_TIMESTAMP:                     C U R R E N T '_' T I M E S T A M P;
CURRENT_USER:                          C U R R E N T '_' U S E R;
CURSOR:                                C U R S O R;
DATA_COMPRESSION:                      D A T A '_' C O M P R E S S I O N;
DATABASE:                              D A T A B A S E;
DBCC:                                  D B C C;
DEALLOCATE:                            D E A L L O C A T E;
DECLARE:                               D E C L A R E;
DEFAULT:                               D E F A U L T;
DELETE:                                D E L E T E;
DENY:                                  D E N Y;
DESC:                                  D E S C;
DESCRIPTION:                           D E S C R I P T I O N;
DISK:                                  D I S K;
DISTINCT:                              D I S T I N C T;
DISTRIBUTED:                           D I S T R I B U T E D;
DOUBLE:                                D O U B L E;
DROP:                                  D R O P;
DUMP:                                  D U M P;
ELSE:                                  E L S E;
END:                                   E N D;
ERRLVL:                                E R R L V L;
ESCAPE:                                E S C A P E;
ETRSTR:                                E T R S T R;
ERROR:                                 E R R O R;
EVENTDATA:                             E V E N T D A T A '(' ')';
EXCEPT:                                E X C E P T;
EXECUTE:                               E X E C (U T E)?;
EXISTS:                                E X I S T S;
EXIT:                                  E X I T;
EXTERNAL:                              E X T E R N A L;
FETCH:                                 F E T C H;
FILE:                                  F I L E;
FILENAME:                              F I L E N A M E;
FILLFACTOR:                            F I L L F A C T O R;
FOR:                                   F O R;
FORCESEEK:                             F O R C E S E E K;
FOREIGN:                               F O R E I G N;
FREETEXT:                              F R E E T E X T;
FREETEXTTABLE:                         F R E E T E X T T A B L E;
FROM:                                  F R O M;
FULL:                                  F U L L;
FUNCTION:                              F U N C T I O N;
GET:                                   G E T;
GOTO:                                  G O T O;
GRANT:                                 G R A N T;
GROUP:                                 G R O U P;
HAVING:                                H A V I N G;
HIST_COUNT:                            C O U N T '_' H I S T;
HIST_COUNT2:                           H I S T '_' D I S T C O U N T;
IDENTITY:                              I D E N T I T Y;
IDENTITYCOL:                           I D E N T I T Y C O L;
IDENTITY_INSERT:                       I D E N T I T Y '_' I N S E R T;
IF:                                    I F;
IN:                                    I N;
INCLUDE:                               I N C L U D E;
INDEX:                                 I N D E X;
INNER:                                 I N N E R;
INSERT:                                I N S E R T;
INSTEAD:                               I N S T E A D;
INTERSECT:                             I N T E R S E C T;
INTO:                                  I N T O;
IS:                                    I S;
JSON_VAL:                              J S O N '_' V A L;
JOIN:                                  J O I N;
KEY:                                   K E Y;
KILL:                                  K I L L;
LEFT:                                  L E F T;
LIFETIME:                              L I F E T I M E;
LIKE:                                  L I K E;
LINENO:                                L I N E N O;
LOAD:                                  L O A D;
LOG:                                   L O G;
MATCHED:                               M A T C H E D;
MERGE:                                 M E R G E;
NATIONAL:                              N A T I O N A L;
NOCHECK:                               N O C H E C K;
NONCLUSTERED:                          N O N C L U S T E R E D;
NONE:                                  N O N E;
NOT:                                   N O T;
NULL:                                  N U L L;
NULLIF:                                N U L L I F;
NESTED_FUNC:                           N E S T E D '_' F U N C | N F | '#';
NF_COUNT:                              N F '_' COUNT | '#' COUNT | P F '_' COUNT;
NF_MAX:                                N F '_' MAX | '#' MAX | P F '_' MAX;
NF_MIN:                                N F '_' MIN | '#' MIN | P F '_' MIN;
NF_SUM:                                N F '_' SUM | '#' SUM | P F '_' SUM;
OF:                                    O F;
OFF:                                   O F F;
OFFSETS:                               O F F S E T S;
ON:                                    O N;
OPEN:                                  O P E N;
OPENDATASOURCE:                        O P E N D A T A S O U R C E;
OPENQUERY:                             O P E N Q U E R Y;
OPENROWSET:                            O P E N R O W S E T;
OPENXML:                               O P E N X M L;
OPTION:                                O P T I O N;
OR:                                    O R | '|' '|';
ORDER:                                 O R D E R;
OUTER:                                 O U T E R;
OVER:                                  O V E R;
PAGE:                                  P A G E;
PARTIAL:                               P A R T I A L;
PASSWORD:                              P A S S W O R D;
PERCENT:                               P E R C E N T;
PIVOT:                                 P I V O T;
PLAN:                                  P L A N;
PRECISION:                             P R E C I S I O N;
PRIMARY:                               P R I M A R Y;
PRINT:                                 P R I N T;
PROC:                                  P R O C;
PROCEDURE:                             P R O C E D U R E;
PUBLIC:                                P U B L I C;
RAISERROR:                             R A I S E R R O R;
RAW:                                   R A W;
READ:                                  R E A D;
READTEXT:                              R E A D T E X T;
RECONFIGURE:                           R E C O N F I G U R E;
REFERENCES:                            R E F E R E N C E S;
RELATED_CONVERSATION:                  R E L A T E D '_' C O N V E R S A T I O N;
RELATED_CONVERSATION_GROUP:            R E L A T E D '_' C O N V E R S A T I O N '_' G R O U P;
REPLICATION:                           R E P L I C A T I O N;
RESTORE:                               R E S T O R E;
RESTRICT:                              R E S T R I C T;
RETURN:                                R E T U R N;
RETURNS:                               R E T U R N S;
REVERT:                                R E V E R T;
REVOKE:                                R E V O K E;
RIGHT:                                 R I G H T;
ROLLBACK:                              R O L L B A C K;
ROWCOUNT:                              R O W C O U N T;
ROWGUIDCOL:                            R O W G U I D C O L;
RULE:                                  R U L E;
SAVE:                                  S A V E;
SCHEMA:                                S C H E M A;
SECURITYAUDIT:                         S E C U R I T Y A U D I T;
SELECT:                                S E L E C T | S E L;
SEMANTICKEYPHRASETABLE:                S E M A N T I C K E Y P H R A S E T A B L E;
SEMANTICSIMILARITYDETAILSTABLE:        S E M A N T I C S I M I L A R I T Y D E T A I L S T A B L E;
SEMANTICSIMILARITYTABLE:               S E M A N T I C S I M I L A R I T Y T A B L E;
SERVER:                                S E R V E R;
SERVICE:                               S E R V I C E;
SESSION_USER:                          S E S S I O N '_' U S E R;
SET:                                   S E T;
SETUSER:                               S E T U S E R;
SHUTDOWN:                              S H U T D O W N;
SOME:                                  S O M E;
SOURCE:                                S O U R C E;
STATISTICS:                            S T A T I S T I C S;
SYSTEM_USER:                           S Y S T E M '_' U S E R;
TABLE:                                 T A B L E;
TABLESAMPLE:                           T A B L E S A M P L E;
TARGET:                                T A R G E T;
TEXTSIZE:                              T E X T S I Z E;
THEN:                                  T H E N;
TO:                                    T O;
TOP:                                   T O P;
TRAN:                                  T R A N;
TRANSACTION:                           T R A N S A C T I O N;
TRIGGER:                               T R I G G E R;
TRUNCATE:                              T R U N C A T E;
TSEQUAL:                               T S E Q U A L;
UNION:                                 U N I O N;
UNIQUE:                                U N I Q U E;
UNPIVOT:                               U N P I V O T;
UPDATE:                                U P D A T E;
UPDATETEXT:                            U P D A T E T E X T;
USE:                                   U S E;
USER:                                  U S E R;
VALUES:                                V A L U E S;
VARYING:                               V A R Y I N G;
VIEW:                                  V I E W;
WAITFOR:                               W A I T F O R;
WHEN:                                  W H E N;
WHERE:                                 W H E R E;
WHILE:                                 W H I L E;
WITH:                                  W I T H;
WITHIN:                                W I T H I N;
WRITETEXT:                             W R I T E T E X T;

// predefined udfs
TO_TIMESTAMP:                          T S;
YEAROF:                                Y E A R O F;
DATEOF:                                D A T E O F;
HOUROF:                                H O U R O F;
WEEKINYEAR:                            W E E K I N Y E A R;
MONTHOF:                               M O N T H O F;
DAYINWEEK:                             D A Y I N W E E K;
SUBSTR:                                S U B S T R;
EXTSTR:                                E X T S T R;


// Additional keywords (they can be id).
ABSOLUTE:                              A B S O L U T E;
ACTION:                                A C T I O N;
AFTER:                                 A F T E R;
ALLOWED:                               A L L O W E D;
ALLOW_SNAPSHOT_ISOLATION:              A L L O W '_' S N A P S H O T '_' I S O L A T I O N;
ANSI_NULLS:                            A N S I '_' N U L L S;
ANSI_NULL_DEFAULT:                     A N S I '_' N U L L '_' D E F A U L T;
ANSI_PADDING:                          A N S I '_' P A D D I N G;
ANSI_WARNINGS:                         A N S I '_' W A R N I N G S;
APPLY:                                 A P P L Y;
ARITHABORT:                            A R I T H A B O R T;
AUTO:                                  A U T O;
AUTO_CLEANUP:                          A U T O '_' C L E A N U P;
AUTO_CLOSE:                            A U T O '_' C L O S E;
AUTO_CREATE_STATISTICS:                A U T O '_' C R E A T E '_' S T A T I S T I C S;
AUTO_SHRINK:                           A U T O '_' S H R I N K;
AUTO_UPDATE_STATISTICS:                A U T O '_' U P D A T E '_' S T A T I S T I C S;
AUTO_UPDATE_STATISTICS_ASYNC:          A U T O '_' U P D A T E '_' S T A T I S T I C S '_' A S Y N C;
AVG:                                   A V G;
BASE64:                                B A S E '64';
BINARY_CHECKSUM:                       B I N A R Y '_' C H E C K S U M;
BULK_LOGGED:                           B U L K '_' L O G G E D;
CALLER:                                C A L L E R;
CAST:                                  (T R Y '_')? C A S T;
CASTLONG:                              T O L O N G;
CATCH:                                 C A T C H;
CHANGE_RETENTION:                      C H A N G E '_' R E T E N T I O N;
CHANGE_TRACKING:                       C H A N G E '_' T R A C K I N G;
CHECKSUM:                              C H E C K S U M;
CHECKSUM_AGG:                          C H E C K S U M '_' A G G;
CLEANUP:                               C L E A N U P;
COMMITTED:                             C O M M I T T E D;
COMPATIBILITY_LEVEL:                   C O M P A T I B I L I T Y '_' L E V E L;
CONCAT:                                C O N C A T;
CONCAT_NULL_YIELDS_NULL:               C O N C A T '_' N U L L '_' Y I E L D S '_' N U L L;
CONTROL:                               C O N T R O L;
COOKIE:                                C O O K I E;
COUNT:                                 C O U N T;
COUNT_BIG:                             C O U N T '_' B I G;
CURSOR_CLOSE_ON_COMMIT:                C U R S O R '_' C L O S E '_' O N '_' C O M M I T;
CURSOR_DEFAULT:                        C U R S O R '_' D E F A U L T;
DATEADD:                               D A T E A D D;
DATEDIFF:                              D A T E D I F F;
DATENAME:                              D A T E N A M E;
DATEPART:                              D A T E P A R T;
DATE_CORRELATION_OPTIMIZATION:         D A T E '_' C O R R E L A T I O N '_' O P T I M I Z A T I O N;
DAYS:                                  D A Y S;
DB_CHAINING:                           D B '_' C H A I N I N G;
DECRYPTION:                            D E C R Y P T I O N;
DEFAULT_FULLTEXT_LANGUAGE:             D E F A U L T '_' F U L L T E X T '_' L A N G U A G E;
DEFAULT_LANGUAGE:                      D E F A U L T '_' L A N G U A G E;
DELAY:                                 D E L A Y;
DELAYED_DURABILITY:                    D E L A Y E D '_' D U R A B I L I T Y;
DELETED:                               D E L E T E D;
DENSE_RANK:                            D E N S E '_' R A N K;
DIALOG:                                D I A L O G;
DIRECTORY_NAME:                        D I R E C T O R Y '_' N A M E;
DISABLE:                               D I S A B L E;
DISABLED:                              D I S A B L E D;
DISABLE_BROKER:                        D I S A B L E '_' B R O K E R;
DURATION_TIME_IN_SEC:                  D U R A T I O N '_' T I M E '_' I N '_' S E C;
DYNAMIC:                               D Y N A M I C;
EMERGENCY:                             E M E R G E N C Y;
ENABLE_BROKER:                         E N A B L E '_' B R O K E R;
ENCRYPTION:                            E N C R Y P T I O N;
ERROR_BROKER_CONVERSATIONS:            E R R O R '_' B R O K E R '_' C O N V E R S A T I O N S;
EXPAND:                                E X P A N D;
FAST:                                  F A S T;
FAST_FORWARD:                          F A S T '_' F O R W A R D;
FILEGROUP:                             F I L E G R O U P;
FILEGROWTH:                            F I L E G R O W T H;
FILESTREAM:                            F I L E S T R E A M;
FIRST:                                 F I R S T;
FOLLOWING:                             F O L L O W I N G;
FORCE:                                 F O R C E;
FORCED:                                F O R C E D;
FORWARD_ONLY:                          F O R W A R D '_' O N L Y;
FULLSCAN:                              F U L L S C A N;
GB:                                    G B;
GETDATE:                               G E T D A T E;
GETUTCDATE:                            G E T U T C D A T E;
GLOBAL:                                G L O B A L;
GO:                                    G O;
GROUPING:                              G R O U P I N G;
GROUPING_ID:                           G R O U P I N G '_' I D;
HADR:                                  H A D R;
HASH:                                  H A S H;
HONOR_BROKER_PRIORITY:                 H O N O R '_' B R O K E R '_' P R I O R I T Y;
HOURS:                                 H O U R S;
IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX: I G N O R E '_' N O N C L U S T E R E D '_' C O L U M N S T O R E '_' I N D E X;
IMMEDIATE:                             I M M E D I A T E;
IMPERSONATE:                           I M P E R S O N A T E;
INCREMENTAL:                           I N C R E M E N T A L;
INPUT:                                 I N P U T;
INSENSITIVE:                           I N S E N S I T I V E;
INSERTED:                              I N S E R T E D;
ISOLATION:                             I S O L A T I O N;
KB:                                    K B;
KEEP:                                  K E E P;
KEEPFIXED:                             K E E P F I X E D;
KEYSET:                                K E Y S E T;
LAST:                                  L A S T;
LEVEL:                                 L E V E L;
LIMIT:                                 L I M I T;
LOCAL:                                 L O C A L;
LOCK_ESCALATION:                       L O C K '_' E S C A L A T I O N;
LOGIN:                                 L O G I N;
LOOP:                                  L O O P;
MARK:                                  M A R K;
MAX:                                   M A X;
MAXDOP:                                M A X D O P;
MAXRECURSION:                          M A X R E C U R S I O N;
MAXSIZE:                               M A X S I Z E;
MESSAGE:                               M E S S A G E;
MB:                                    M B;
MEMORY_OPTIMIZED_DATA:                 M E M O R Y '_' O P T I M I Z E D '_' D A T A;
MIN:                                   M I N;
MIN_DURATION_IN_MS:                    M I N '_' D U R A T I O N '_' I N '_' M S;
MIN_FREQ:                              M I N '_' F R E Q;
MINUTES:                               M I N U T E S;
MIN_ACTIVE_ROWVERSION:                 M I N '_' A C T I V E '_' R O W V E R S I O N;
MIXED_PAGE_ALLOCATION:                 M I X E D '_' P A G E '_' A L L O C A T I O N;
MODIFY:                                M O D I F Y;
MULTI_USER:                            M U L T I '_' U S E R;
NAME:                                  N A M E;
NESTED_TRIGGERS:                       N E S T E D '_' T R I G G E R S;
NEW_BROKER:                            N E W '_' B R O K E R;
NEXT:                                  N E X T;
NOCOUNT:                               N O C O U N T;
NOEXPAND:                              N O E X P A N D;
NON_TRANSACTED_ACCESS:                 N O N '_' T R A N S A C T E D '_' A C C E S S;
NORECOMPUTE:                           N O R E C O M P U T E;
NO:                                    N O;
NO_WAIT:                               N O '_' W A I T;
NTILE:                                 N T I L E;
NUMBER:                                N U M B E R;
NUMERIC_ROUNDABORT:                    N U M E R I C '_' R O U N D A B O R T;
OFFLINE:                               O F F L I N E;
OFFSET:                                O F F S E T;
ONLINE:                                O N L I N E;
ONLY:                                  O N L Y;
OPTIMISTIC:                            O P T I M I S T I C;
OPTIMIZE:                              O P T I M I Z E;
OUT:                                   O U T;
OUTPUT:                                O U T P U T;
OWNER:                                 O W N E R;
PAGE_VERIFY:                           P A G E '_' V E R I F Y;
PARAMETERIZATION:                      P A R A M E T E R I Z A T I O N;
PARTITION:                             P A R T I T I O N;
PAST:                                  P A S T | L A S T;
PATH:                                  P A T H;
PLAY_SESSION_COUNT:                    P L A Y '_' S E S S I O N '_' C O U N T;
PRECEDING:                             P R E C E D I N G;
PRIOR:                                 P R I O R;
PRIVILEGES:                            P R I V I L E G E S;
QUOTED_IDENTIFIER:                     Q U O T E D '_' I D E N T I F I E R;
RANGE:                                 R A N G E;
RANK:                                  R A N K;
READONLY:                              R E A D O N L Y;
READ_COMMITTED_SNAPSHOT:               R E A D '_' C O M M I T T E D '_' S N A P S H O T;
READ_ONLY:                             R E A D '_' O N L Y;
READ_WRITE:                            R E A D '_' W R I T E;
REBUILD:                               R E B U I L D;
RECOMPILE:                             R E C O M P I L E;
RECEIVE:                               R E C E I V E;
RECOVERY:                              R E C O V E R Y;
RECURSIVE_TRIGGERS:                    R E C U R S I V E '_' T R I G G E R S;
RELATIVE:                              R E L A T I V E;
REMOTE:                                R E M O T E;
REPEATABLE:                            R E P E A T A B L E;
RESTRICTED_USER:                       R E S T R I C T E D '_' U S E R;
ROBUST:                                R O B U S T;
ROOT:                                  R O O T;
ROW:                                   R O W;
ROWGUID:                               R O W G U I D;
ROWS:                                  R O W S;
ROW_NUMBER:                            R O W '_' N U M B E R;
SAMPLE:                                S A M P L E;
SCHEMABINDING:                         S C H E M A B I N D I N G;
SCROLL:                                S C R O L L;
SCROLL_LOCKS:                          S C R O L L '_' L O C K S;
SECONDS:                               S E C O N D S;
SELF:                                  S E L F;
SEND:                                  S E N D;
SERIALIZABLE:                          S E R I A L I Z A B L E;
SHOWPLAN:                              S H O W P L A N;
SIMPLE:                                S I M P L E;
SINGLE_USER:                           S I N G L E '_' U S E R;
SIZE:                                  S I Z E;
SNAPSHOT:                              S N A P S H O T;
START_TIME_IN_MS:                      S T A R T '_' T I M E '_' I N '_' M S;
SPATIAL_WINDOW_MAX_CELLS:              S P A T I A L '_' W I N D O W '_' M A X '_' C E L L S;
STATIC:                                S T A T I C;
STATS_STREAM:                          S T A T S '_' S T R E A M;
STDEV:                                 S T D E V;
STDEVP:                                S T D E V P;
SUM:                                   S U M;
SYMMETRIC:                             S Y M M E T R I C;
TAKE:                                  T A K E;
TARGET_RECOVERY_TIME:                  T A R G E T '_' R E C O V E R Y '_' T I M E;
TB:                                    T B;
TEXTIMAGE_ON:                          T E X T I M A G E '_' O N;
THROW:                                 T H R O W;
TIES:                                  T I E S;
TIME:                                  T I M E;
TIMER:                                 T I M E R;
TIMEOUT:                               T I M E O U T;
TORN_PAGE_DETECTION:                   T O R N '_' P A G E '_' D E T E C T I O N;
TRANSFORM_NOISE_WORDS:                 T R A N S F O R M '_' N O I S E '_' W O R D S;
TRUSTWORTHY:                           T R U S T W O R T H Y;
TRY:                                   T R Y;
TWO_DIGIT_YEAR_CUTOFF:                 T W O '_' D I G I T '_' Y E A R '_' C U T O F F;
TYPE:                                  T Y P E;
TYPE_WARNING:                          T Y P E '_' W A R N I N G;
UNBOUNDED:                             U N B O U N D E D;
UNCOMMITTED:                           U N C O M M I T T E D;
UNKNOWN:                               U N K N O W N;
UNLIMITED:                             U N L I M I T E D;
USING:                                 U S I N G;
VAR:                                   V A R;
VARP:                                  V A R P;
VIEWS:                                 V I E W S;
VIEW_METADATA:                         V I E W '_' M E T A D A T A;
WORK:                                  W O R K;
XML:                                   X M L;
XMLNAMESPACES:                         X M L N A M E S P A C E S;

DOLLAR_ACTION:                         '$' A C T I O N;

DOUBLE_QUOTE_ID:    '"' ~'"'+ '"';
//SQUARE_BRACKET_ID:  '[' ~']'+ ']';
ID:                  ( [a-zA-Z_#] | FullWidthLetter) ( [a-zA-Z_#$@0-9] | FullWidthLetter )*;
TIME_ZONE_ID:        LETTER LETTER LETTER;

STRING:              N? '\'' (~'\'' | '\'\'')* '\'';
BINARY:              '0' X HEX_DIGIT*;
FLOAT:               DEC_DOT_DEC;
REAL:                DEC_DOT_DEC (E [+-]? DEC_DIGIT+)?;
DECIMAL:             DEC_DIGIT+;

EQUAL:               '=';
GREATER:             '>';
LESS:                '<';
EXCLAMATION:         '!';

SPACE:              [ \t\r\n]+    -> skip;
COMMENT:            '/*' .*? '*/' -> channel(HIDDEN);
LINE_COMMENT:       '--' ~[\r\n]* -> channel(HIDDEN);

fragment LETTER:       [a-zA-Z_];
fragment DEC_DOT_DEC:  (DEC_DIGIT+ '.' DEC_DIGIT+ |  DEC_DIGIT+ '.' | '.' DEC_DIGIT+);
fragment HEX_DIGIT:    [0-9A-Fa-f];
fragment DEC_DIGIT:    [0-9];

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];
fragment FullWidthLetter
    : '\u00c0'..'\u00d6'
    | '\u00d8'..'\u00f6'
    | '\u00f8'..'\u00ff'
    | '\u0100'..'\u1fff'
    | '\u2c00'..'\u2fff'
    | '\u3040'..'\u318f'
    | '\u3300'..'\u337f'
    | '\u3400'..'\u3fff'
    | '\u4e00'..'\u9fff'
    | '\ua000'..'\ud7ff'
    | '\uf900'..'\ufaff'
    | '\uff00'..'\ufff0'
    // | '\u10000'..'\u1F9FF'  //not support four bytes chars
    // | '\u20000'..'\u2FA1F'
    ;