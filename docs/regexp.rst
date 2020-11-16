Regular Expressions Cheat Sheet
===============================

Important difference: filename matching (filename.*) matches entire string, while regular expressions, by default, match a sub-string.

=============================    ================                  ==================
Meaning                          Filename pattern                  Regular expression
=============================    ================                  ==================
Single wildcard character        ?                                 .
Match dot character              filename.type                     filename\\\\.type
Wildcard substring               \*                                .*
Character in list                [abc]                             [abc]
Character in range               [0-9]                             [0-9]
Character not in list            [!ab^c]                           [^ab^c]
Zero or more occurances                                            (abcd)* x*
One or more occurances                                             (abcd)+ x+
Optional                                                           (abcd)? x?
Match at the beginning           pattern*                          ^pattern
Match at the end                 \*pattern                          pattern$
Match entire string              pattern                           ^pattern$
=============================    ================                  ==================



