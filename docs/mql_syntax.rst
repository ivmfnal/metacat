MQL Syntax
==========

File queries
------------

.. code-block::

        <file query>: files [from [dataset|datasets] <dataset selector list> [,...]]
                | <file query> where <metadata expression>]
                | <file query> skip <integer>
                | <file query> limit <integer>
                | query <saved query namespace>:<saved query name>
                | filter <filter name>( <parameter> [,...] ) ( <file query> [,...] )
                | union ( <file query> [,...] )
                | join ( <file query> [,...] )
                | <file query> - <file query>
                | children ( <file query> )
                | parents ( <file query> )
                | ( <file query> )

Dataset queries
---------------

.. code-block::

        <dataset query>: datasets <dataset selector list>
                
        <dataset selector list>: <dataset selector with provenance and meta expression> [, ...]
                
        <dataset selector with provenance and meta expression>: <dataset selector with provenance> [having <metadata expression>]
                
        <dataset selector with provenance>: <dataset name selector> [with children [recursively]]
                
        <dataset name selector>: <namespace>:<name>
                | matching <namespace>:<name pattern SQL style>                         # % - match any substring, _ - match any single character
                | matching regexp <namespace>:"<name pattern regexp style>"

Metadata expressions
--------------------

.. code-block::

        <metadata expression>: <scalar> <cmp op> <constant>
                | <attribute name> [[not] present]
                | <constant> [not] in <attribute>
                | <scalar> [not] in <constant> : <constant>                     # range of values, can be ints or floats
                | <scalar> [not] in ( <constant> [,...] )                       # enumeration of constants, ints, floats, strings, bool
                | ( <metadata expression > )
                | ! <metadata expression>
                | <metadata expression> and <metadata expression>               # and has higher priority than or
                | <metadata expression> or <metadata expression>

        <scalar>: <attribute name>                                              # assumed to be scalar
                | <attribute name> [ <index> ]                                  # assumed to be array
                | <attribute name> [ STRING ]                                   # assumed to be dictionary
                
        <index>: INTEGER
                | any
                | all

        <cmp op>: = 
                | ==                                                            # synonym to "="
                | != 
                | < 
                | <= 
                | > 
                | >= 
                | ~                                                             # regexp matching
                | ~*                                                            # ignore case
                | !~                                                            # not match
                | !~*                                                           # not match ignoring case

        <constant>: SIGNED_INT
                | FLOATING_POINT
                | "STRING"                                                      # any quoted string
                | SAFE_STRING                                                   # string of: A-Z, a-z, 0-9, :%$@_^.%*?-
                | true 
                | false
                | null
                
        <attribute name>: <name>                                                # fixed file attributes (namespace, name, creator, size, ...)
                | <category>.<name>                                             # category can have subcategory: <category>.<subcategory>...<name>
        
        <category>: <name>
                | <category>.<name>
        
        <name>: LETTER ("_"|"-"|"."|LETTER|DIGIT|"/")*