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
    | "matching" unquoted_pattern
    | "matching" "regexp" quoted_pattern

unquoted_pattern:    (FNAME ":")? UNQUOTED_STRING                   -> did_pattern

quoted_pattern:    (FNAME ":")? STRING                              -> did_pattern

dataset_spec_list : dataset_spec ("," dataset_spec)*

?simple_dataset_query : dataset_query_with_subsets

simple_dataset_query_list : simple_dataset_query ("," simple_dataset_query)*        -> dataset_query_list

"""


