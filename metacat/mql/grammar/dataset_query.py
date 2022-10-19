DatasetQuery = """

top_dataset_query       :    dataset_query

?dataset_query   :   "datasets" basic_dataset_query_list

basic_dataset_query_list: basic_dataset_query ("," basic_dataset_query)*

?basic_dataset_query:    dataset_query_with_subsets
    | dataset_query_with_subsets "where" meta_exp                   -> dataset_add_where
    
?dataset_query_with_subsets : dataset_selector
    | dataset_selector dataset_provenance_op                        -> dataset_add_subsets

!dataset_provenance_op: "with" "children" "recursively"?

simple_dataset_query: qualified_name
    | dataset_pattern

"""


