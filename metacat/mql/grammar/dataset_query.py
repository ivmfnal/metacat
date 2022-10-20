DatasetQuery = """

top_dataset_query       :    dataset_query

?dataset_query   :   "datasets" dataset_query_list

dataset_query_list: basic_dataset_query ("," basic_dataset_query)*            -> dataset_query_list

?basic_dataset_query:    dataset_query_with_subsets
    | dataset_query_with_subsets "having" meta_exp                   -> dataset_add_where
    
?dataset_query_with_subsets : dataset_spec
    | dataset_spec dataset_provenance_op                            -> add_subsets

!dataset_provenance_op: "with" "children" "recursively"?

!dataset_spec: qualified_name
    | "matching" sql_pattern
    | "matching" "regexp" regexp_pattern

sql_pattern:    (FNAME ":")? UNQUOTED_STRING                   
regexp_pattern:    (FNAME ":")? STRING                              

dataset_spec_list : dataset_spec ("," dataset_spec)*

?simple_dataset_query : dataset_query_with_subsets

simple_dataset_query_list : simple_dataset_query ("," simple_dataset_query)*        -> dataset_query_list

"""


