QueryQuery = """

!top_query_query       :    "queries" ("matching" query_name_match)? ("where" meta_exp)?

!query_name_match : sql_pattern
    | "regexp" regexp_pattern

// dataset attributes
QUERY_ATTR_NAME: "namespace" | "name" | "creator" | "description" | "created_timestamp" | "source"
"""


