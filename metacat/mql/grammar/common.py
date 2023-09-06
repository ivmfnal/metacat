Common = """

?query:  top_file_query             
    | top_dataset_query
    | top_query_query

qualified_name:     (FNAME ":")? FNAME

qualified_name_list:   qualified_name ("," qualified_name)*

?did:    FNAME ":" FNAME

fid_list:  FID ("," FID)*

//?did_list:  did ("," did)*

param_def_list :  param_def ("," param_def)*

param_def: FNAME "=" constant

?meta_exp:   meta_or                                                           

meta_or:    meta_and ( "or" meta_and )*

meta_and:   term_meta ( "and" term_meta )*

?term_meta:  scalar CMPOP constant                  -> cmp_op
    | scalar "in" constant ":" constant             -> in_range
    | scalar "not" "in" constant ":" constant       -> not_in_range
    | scalar "in" "(" constant_list ")"             -> in_set
    | scalar "not" "in" "(" constant_list ")"       -> not_in_set
    | META_NAME "present"                               -> present                   
    | META_NAME "not" "present"                         -> not_present                   
    | constant "in" META_NAME                           -> constant_in
    | constant "not" "in" META_NAME                     -> constant_not_in
    | "(" meta_exp ")"                              
    | "!" term_meta                                 -> meta_not
    | "exists" STRING                               -> json_path

scalar: META_NAME                                       -> meta_attribute
        | OBJECT_ATTRIBUTE                              -> object_attribute
        | META_NAME "[" "all" "]"                       -> array_all
        | META_NAME "[" "any" "]"                       -> array_any
        | META_NAME "[" (SIGNED_INT|STRING) "]"         -> subscript
        | "len" "(" META_NAME ")"                       -> array_length

constant_list:    constant ("," constant)*                    

constant : SIGNED_FLOAT                             -> float_constant                      
    | STRING                                        -> string_constant
    | SIGNED_INT                                    -> int_constant
    | BOOL                                          -> bool_constant
    | UNQUOTED_STRING                               -> string_constant
    | "datetime" "(" (STRING|UNQUOTED_STRING) ")"   -> datetime_constant
    | "date" "(" (STRING|UNQUOTED_STRING) ( "," (STRING|UNQUOTED_STRING) )? ")"         -> date_constant

index:  STRING
    | SIGNED_INT

META_NAME: WORD ("." WORD)+                            // meta attribute has to have a dot in the name
OBJECT_ATTRIBUTE: WORD

FNAME: LETTER ("_"|"-"|"."|LETTER|DIGIT|"/")*

FID: ("_"|"-"|"."|LETTER|DIGIT|"/")+

WORD: LETTER ("_"|LETTER|DIGIT)*

CMPOP:  "<" "="? | "!"? "=" "="? | "!"? "~" "*"? | ">" "="? | "like"            //# like is not implemented yet

BOOL: "true"i | "false"i

STRING : /("(?!"").*?(?<!\\\\)(\\\\\\\\)*?"|'(?!'').*?(?<!\\\\)(\\\\\\\\)*?')/i
SAFE_CHARACTER : /[a-z0-9$@_.-]/i
UNQUOTED_STRING : SAFE_CHARACTER+
PATTERN_CHARACTER : (SAFE_CHARACTER|/[*?^%]/)
PATTERN : PATTERN_CHARACTER+

%import common.CNAME
%import common.SIGNED_INT
%import common.SIGNED_FLOAT

%import common.WS
%import common.LETTER
%import common.DIGIT
%ignore WS


"""


