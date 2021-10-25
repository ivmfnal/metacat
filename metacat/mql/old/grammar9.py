MQL_Grammar = """
query:  ("with" param_def_list)? params_applied_query

?params_applied_query:  file_query             
    | dataset_query                            

file_query: file_query2                                   
    | file_query "-" file_query2                          -> subtract

!file_query2: filterable_file_query ("where" meta_exp)? ("limit" SIGNED_INT)?       -> file_query

filterable_file_query:  "files" ("from" datasets_selector)? -> basic_file_query
    |   "union" "(" file_query_list ")"                  -> union
    |   "[" file_query_list "]"                          -> union
    |   "join"  "(" file_query_list ")"                  -> join
    |   "{" file_query_list "}"                          -> join
    |   "parents" "(" file_query ")"                     -> parents_of
    |   "children" "(" file_query ")"                    -> children_of
//    |   file_query "-" file_query                        -> subtract
    |   "(" file_query ")"                               -> s_
    |   "filter" FNAME "(" constant_list ")" "(" file_query_list ")"       -> filter
    |   "query" qualified_name                           -> named_query
    
file_query_list: file_query ("," file_query)*     

!datasets_selector: dataset_spec_list ("with" "children" "recursively"?)? ("having" meta_exp)?

dataset_spec_list: dataset_spec ("," dataset_spec)* 

dataset_spec:    qualified_name
    | dataset_pattern

dataset_query:  "datasets" datasets_selector

dataset_pattern:    (FNAME ":")? STRING

qualified_name:     (FNAME ":")? FNAME

param_def_list :  param_def ("," param_def)*

param_def: CNAME "=" constant

?meta_exp:   meta_or                                                           

meta_or:    meta_and ( "or" meta_and )*

meta_and:   term_meta ( "and" term_meta )*

term_meta:  ANAME CMPOP constant                    -> cmp_op
    | ANAME "present"?                              -> present_op                   //# new
    | ANAME "[" index "]" CMPOP constant            -> subscript_cmp_op             //# new
    | constant "in" ANAME                           -> in_op
    | ANAME "contains" constant                     -> contains_op                  //# new
    | "(" meta_exp ")"                              -> s_
    | "!" term_meta                                 -> meta_not
    | "eval" FNAME "(" constant_list ")"            -> eval                         //# new
    
constant_list:    constant? ("," constant)*                    

constant : SIGNED_FLOAT                             -> float_constant                      
    | STRING                                        -> string_constant
    | SIGNED_INT                                    -> int_constant
    | BOOL                                          -> bool_constant

index:  STRING
    | SIGNED_INT

ANAME: WORD ("." WORD)*

FNAME: LETTER ("_"|"-"|"."|LETTER|DIGIT)*

WORD: LETTER ("_"|LETTER|DIGIT)*

CMPOP:  "<" "="? | "!"? "=" "="? | "!"? "~~" "*"? | ">" "="? 

BOOL: "true"i | "false"i

STRING : /("(?!"").*?(?<!\\\\)(\\\\\\\\)*?"|'(?!'').*?(?<!\\\\)(\\\\\\\\)*?')/i

%import common.CNAME
%import common.SIGNED_INT
%import common.SIGNED_FLOAT

%import common.WS
%import common.LETTER
%import common.DIGIT
%ignore WS
"""


