FileQuery = """

top_file_query          :    file_query

?file_query: meta_filter                                  
    | file_query "-" meta_filter                          -> minus

?meta_filter: file_query_exression "where" meta_exp     
    |   file_query_exression                             

?file_query_exression:  file_query_term                   
    |   "union" "(" file_query_list ")"                  -> union
    |   "[" file_query_list "]"                          -> union
    |   "join"  "(" file_query_list ")"                  -> join
    |   "{" file_query_list "}"                          -> join
    |   "parents" "(" file_query ")"                     -> parents_of
    |   "children" "(" file_query ")"                    -> children_of
    |   file_query "limit" SIGNED_INT                    -> limit              
    |   file_query "skip" SIGNED_INT                     -> skip
    |   file_query "ordered"                             -> ordered
    |   "(" file_query ")"           

file_query_term: "files" ("from" "datasets"? dataset_query_list)?                   -> basic_file_query
    |   "filter" FNAME "(" filter_params ? ")" "(" file_query_list ")"              -> filter
    |   "files" "selected" "by"? qualified_name                                     -> named_query
    |   file_list

!file_list: ("fids"|"fid") fid_list
    |   ("files"|"file") qualified_name_list

filter_params : params_list
    |   (params_list ",")? param_def_list

params_list : constant_list         // convert date, datetime to floats

file_query_list: file_query ("," file_query)*     

// file attributes
FILE_ATTR_NAME: ("id" | "namespace" | "name" | "creator" | "updated_by" | "created_timestamp" | "updated_timestamp" | "retired" | "retired_by" | "retired_timestamp" )

"""