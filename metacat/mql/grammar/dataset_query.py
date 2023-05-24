DatasetQuery = """

top_dataset_query       :    "datasets" dataset_query

?dataset_query   :    dataset_query_with_subsets
    | dataset_query_with_subsets "having" meta_exp
    
?dataset_query_with_subsets : dataset_spec
    | dataset_spec dataset_provenance_op

!dataset_provenance_op: "with" "children" "recursively"?

!dataset_spec:                  // empty = all datasets
    | qualified_name
    | "matching" "regexp" regexp_pattern
    | "matching" sql_pattern

sql_pattern:    (FNAME ":")? UNQUOTED_STRING                   
regexp_pattern:    (FNAME ":")? STRING                              

?simple_dataset_query : dataset_query_with_subsets

// dataset attributes
DATASET_ATTR_NAME: "namespace" | "name" | "parent_namespace" | "parent_name" | "creator" | "description" | "created_timestamp" | "updated_timestamp"
    | "frozen" 
    | "monotonic"
"""


