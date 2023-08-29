DatasetQuery = """

top_dataset_query       :    "datasets" dataset_query_list

dataset_query_list: dataset_query ("," dataset_query)*            -> dataset_query_list

?dataset_query   :    dataset_query_with_subsets
    | dataset_query_with_subsets "having" meta_exp          -> dataset_add_where
    
?dataset_query_with_subsets : dataset_spec
    | dataset_spec dataset_provenance_op                    -> dataset_add_subsets

!dataset_provenance_op: "with" "subsets" "recursively"?

!dataset_spec:  did
    | "matching" sql_pattern
    | "matching" "regexp" regexp_pattern

sql_pattern:       FNAME ":" PATTERN                   
regexp_pattern:    FNAME ":" STRING                       

?simple_dataset_query : dataset_query_with_subsets

// dataset attributes
DATASET_ATTR_NAME: "namespace" | "name" | "parent_namespace" | "parent_name" | "creator" | "description" | "created_timestamp" | "updated_timestamp"
    | "frozen" 
    | "monotonic"
"""


