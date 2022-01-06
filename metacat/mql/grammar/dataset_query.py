DatasetQuery = """

top_dataset_query       :    dataset_query

dataset_query   :   "datasets" dataset_query_expression

?dataset_query_expression:   dataset_query_term 
    |   dataset_query_expression having_op
    |   dataset_query_expression provenance_op
    
?dataset_expression_list: dataset_query_expression ("," dataset_query_expression)
    
having_op: "having" meta_exp

!provenance_op: "with" "children" "recursively"?

dataset_query_term: qualified_name
    | dataset_pattern
    | "(" dataset_expression_list ")"

"""


