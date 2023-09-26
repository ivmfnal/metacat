MQL - Metadata Query Language
=============================

Introduction
~~~~~~~~~~~~
One of the functions of the Metadata Database is to produce list of files matching a set of crieria specidied
by the user. The product has its own simple language to write these queries in called MQL (pronpounced: MEE-quel,
like "sequel", but with M). MQL is a language to describe queries against the metadata database.
A query produces a set of files. By deafult, the order of files in the returned set is not guaranteed and can not be
relied on. See  :ref:`Query Results Ordering <ordering>` below for more details.

There are 2 classes of queries - file queries and dataset queries. File queries return list of files
matching specified criteria and dataset queries list datasets.

File Queries
~~~~~~~~~~~~

Simple Query
------------

The simplest MQL query you can write is a *Dataset Query*, which looks like this:

.. code-block:: sql

        files from MyScope:MyDataset
        
This query simply returns all the files included in the dataset "MyScope:MyDataset".

You can also specify multiple datasets in the same query:

.. code-block:: sql

        files from MyScope:MC1, MyScope:MC2, AnotherScope:MC

Also, you can use wildcards in the dataset name (but not in the dataset namespace).

.. code-block:: sql

        files from datasets matching MyScope:"MC%" 
            where ...

        files from datasets matching MyScope:"MC*" 
            where ...

        files from datasets matching regexp MyScope:"MC.*" 
            where ...

The `regexp` keyword tells MQL to use regular expression to match the dataset name. Otherwise, it will use
SQL wildcard notation (% - any substring, _ - any single character) or UNIX filename match notation
(* - any substring, ? - any single character).

The `from <dataset specification>` clause is optional.
If do not want to limit the query to one or more datasets, you can omit it:

.. code-block:: sql

    files where core.data_type="mc"

but this can take long time because it will scan the entire MetaCat database.

To select files by their explicit namespaces and names:

.. code-block:: sql

    files my_namespace:file_name.data, file1.data, file2.data, 
                anoher_namespace:file3.data


or by their file ids:

.. code-block:: sql

    fids 1234, 12354, 12363

This type of queries can be used to get metadata for known files.


Metadata Filtering
------------------

Results of any query can be filtered by adding some metadata criteria expression, called *meta-filter*. For example, the following query
returns all the files from the ``MyScope:MyDataset``:

.. code-block:: sql

        files from MyScope:MyDataset

If we add a meta-filter to this query, then the results will be limited to those mathich the specified crireria:

.. code-block:: sql

        files from MyScope:MyDataset
                where params.x > 0.5
                
A meta-filter can be more complicated:

.. code-block:: sql

        files from MyScope:MyDataset                                # (A)
            where params.x > 0.5 and params.x < 1.5 
                    and data.run = 123 
                    and ( data.type="MC" or data.type="Data" )

Meta-filters can be chained. The following query is equivalent to the query above:

.. code-block:: sql

        files from MyScope:MyDataset                                # (B)
            where params.x > 0.5 and params.x < 1.5 
                where data.run = 123 
                    where ( data.type="MC" or data.type="Data" )

In fact, MQL compiler always merges subsequent meta-filters into single meta-filter, so, behind the scene, query (B) will be converted to (A) first
and then further compiled and executed.

File/Dataset Attributes
-----------------------
Each file and dataset has a fixed set of attributes. File and dataset attributes can be used in MQL query
just like metadata parameters. MQL recognizes the attributes by absence if a dot in their names.

The following are file attributes which can be used in a file query:

     * id
     * namespace
     * name
     * creator
     * updated_by
     * created_timestamp
     * updated_timestamp
     * retired 
     * retired_by
     * retired_timestamp
 
Dataset attributes:

     * namespace
     * name
     * parent_namespace
     * parent_name
     * creator
     * description
     * created_timestamp
     * frozen
     * monotonic
 
Examples of queries uaing file and dataset attributes:

.. code-block:: sql

    files from scope:dataset where data.type = monte-carlo and creator=joe
    
    datasets matching scope:data_* having frozen=false and math.pi=3.14


Safe Strings
------------
String constants containing only letters, digits and symbols ``$@_.-`` (safe string literals) can be entered without
enclosing quotes. So the following queries are equivalent:

.. code-block:: sql

    files from scope:dataset where data.type = monte-carlo
    files from scope:dataset where data.type = "monte-carlo"

Unquoted literals which can be interpreted as numeric or boolean constants
will be interpreted as such. If you need to represent a string, which looks like a decimal representation of
a number, you will have to put it in quotes, e.g.:

.. code-block:: sql

	files from scope:dataset where software.version = 1.2      # will be comparing to floating point 1.2
	files from scope:dataset where software.version = "1.2"    # will be comparing to string "1.2"

File Provenance
---------------
MetaCat supports the parent/child relationship between files. A file can have 0 or more child files and 0 or more parent files.
To get list of parents or children of all files matching certain criteria, use ``parents`` and ``children`` keywords:

.. code-block:: sql

        parents (
            files from MyScope:MyDataset
                where x > 0.5 and x < 1.5 
                        and run = 123 
                        and ( type="MC" or type="Data" )
        )

        children (
            files from MyScope:MyDataset
                where params.x > 0.5 and params.x < 1.5 
                        and dara.run = 123 
                        and ( data.type="MC" or data.type="Data" )
        )

You can use MQL to get parents or children of a single 


If you want to get a list of files without any children, you can use this trick with file set subtraction:

.. code-block:: sql

        files from MyScope:MyDataset 
        - parents (
            children (
                files from MyScope:MyDataset
            )
        )


                
Combining Queries
-----------------

Queries can be combined using boolean operations *union*, *join*, and subtraction to produce new queries:

.. code-block:: sql

        union(
                files from MC:Cosmics
                        where params.p > 0.5 and params.p < 1.5 
                files from MC:Beam where params.e = 10
        )
        
This query will return files from both datasets. Even if the individual queries happen to produce overallping
sets of files, each file will appear only *once* in the results of the *union* query.

Queries can be *joined* to procude the intersection of the results of individual queries:

.. code-block:: sql

        join(
                files from MC:All
                        where params.p > 0.5 and params.p < 1.5 
                files from MC:All
                        where params.e = 10
        )
        
Of course this is equivalent to:

.. code-block:: sql

        files from MC:All
                where params.p > 0.5 and params.p < 1.5 and params.e = 10
        
Queries can be subtracted from each other, which means the resulting set will be boolean subtraction of second query
result set from the first:

.. code-block:: sql

        files from MC:Beam where params.e1 > 10 - files from MC:Exotics
        
Although is it not necessary in this example, you can use parethesis and white space to make the query more readable:

.. code-block:: sql

        (files from MC:Beam where params.e1 > 10) 
        - (files from MC:Exotics where data.type = "abcd")
        
Also, you can use square and curly brackets as an alternative to using explicit words "union" and "join" respectively.
The following two queries are equivalent:

.. code-block:: 

        union (
                files from s:A,
                join(
                        files from s:B,
                        files from s:C
                )
        )

        [
                files from s:A,
                {
                        files from s:B,
                        files from s:C
                }
        ]

External Filters
----------------

The Meatadata Database Query Engine lets the user add custom Python code to be used as a more complicated
operations on the file sets. They in the Query Language, they are invoked using "filter" keyword:

.. code-block:: sql

        filter sample(0.5)( files from s:A )
        
Here, *filter* the the keyword, *sample* is the name of the Python function to be used to filter the results
of the argument query (simple "files from s:A" query in this case). As you can see, you can pass some
parameters to the function (the number 0.5).


Standard MetaCat Filters
________________________

MetaCat provides several general purpose filters:

**every_nth** the filter has 2 integer parameters - ``n`` and ``i`` and takes single file set as input.
It returns every ``n``-th file, starting from ``i``. For example, if a dataset has files A0, A1, A2, A3, A4, A5, ...,
and the query looks like this:

.. code-block:: sql

        filter every_nth(3,1)( files from s:A )
        
then the filter will return files A1, A4, ...

Note that MetaCat does not guarantee that the underlying query (files from s:A) will always return files
in the same order. Therefore, strictly speaking, every_nth filter may return different results even if the
source dataset does not change.

If you need more reproducibility, you can use ``hash`` filter:

**hash** filter has the same 2 parameters as the ``every_nth`` filter (``n`` and ``i``) and takes single input file set, but it
uses hash of file id modulo ``n`` to compare to ``i`` to select approximately every ``n``-th file. Notice that the number
of files selected by this filter may differ significantly from ``1/n`` for small file sets.

It is guaranteed that the results of the ``hash`` filter with the same ``n`` and different ``i`` will never intersect.
The same is not necesarily true for ``every_nth`` filter simply because the order, in which files are seen by the filter
may change from query to query, although this is highly unlikely.

**sample** the filter has one argument - a floating point fraction ``f`` from 0 to 1. It works the same way as the ``every_nth`` in the
sense that ``sample`` selects ``1/n`` files from the set, starting from first. The following queries will produce the same results:

.. code-block:: sql

        filter sample(0.01)( files from s:A )
        filter every_nth(100,0)( files from s:A )

**mix** - ``mix`` filter can be used to pick files from multiple datasets. It takes variable number of floating point arguments (``fractions``)
and the same number of input file sets. The files from the input sets will be picked proportinally to the ``fractions``. Fractions do not have
to add up to 1.0. The filter will run until it reaches the end of one of the input datasets. For example:

.. code-block:: sql

        filter sample(1,2,5)(
            files from s:A, 
            files from s:B, 
            files from s:C
        )
        
The output will have approximately 2 files from dataset B and 5 files from dataset C for every file from dataset A.

Even if a file appears in more than one of the input file sets, it will not be returned several times.

User Defined Filters
____________________

User-defined filters are used to extend MetaCat functionality and as a way to access external metadata and use it to further filter the file sets
and to inject metadata from external sources into MetaCat query.

A user can define their own filters by supplying a class derived from ``MetaCatFiler`` class imported from ``metacat.filters``.
The class may have a constructor, which receives a dictionary with configuration parameters and must have a method called ``filter``:

.. code-block:: python

    from metacat.filters import MetaCatFiler
    
    class MyFilter(MetaCatFiler):
    
        def __init__(self, config):
            self.DataSource = ...

        def filter(self, inputs, *params, **key_value):
            input_set = inputs[0]
            
            for f in input_set:
                external_data = self.DataSource.get(f)
                if ...:
                    f.Metadata["extra_field"] = some_data
                    yield f

First argument of the ``filter`` method is the list of one or more input file sets. They are results of MQL subqueries passed to the filter as inputs. 
Each input file set is an iterable, not lists. If necessary, the input file set can be converted to a list as ``list(file_set)``, but that needs to
be done with caution because that will force fetching the entire file set into memory, and that can be very big.

After first parameter, the ``filter`` method can accept some additional positional and keywird parameters passed from MQL. For example, MQL query like this:

.. code-block::

    filter my_filter(3, 'test', pi=3.14, e=2.718) (
        files from user:dataset_a,
        files from group:dataset_b where x=5
    )

will call the filter() method with the following arguments:

.. code-block:: python

    ...
    filter_object.filter([file_set_a, file_set_b], 3, "test", pi=3.14, e=2.18)
    ...

The ``filter`` method is expected to generate a list of file object from the input file sets, possibly augmenting their metadata with some
data.

MetaCat will create the filter object only once and then call its ``filter`` method for each query. Thus, the filter object may have some persistent state,
but that feature should be used with caution because:

    * MetaCat server runs in multiple instances on multiple servers, and the instances do not communicate with each other.
    * MetaCat server instance is a multithreaded process and queries are executed on concurrent threads, so some sort of inter-thread synchronization mechanism may need to be used.

Common Namesaces
----------------

Typically (but not necessarily), all the datasets mentioned in a query refer to the same namespace.
You can avoid repeting the same namespace using "with" clause. The following are equivalent:

.. code-block:: 

        with namespace="s"
        {
                files from B,
                files from C
        }

        {
                files from s:B,
                files from s:C
        }

Each "with" clause has its scope limited to the immediate query it is attached to. For example, the following query
is invalid:

.. code-block:: sql

        with namespace="s"      
                files from A - files from B

It is invalid becaise the "with" clause applies only to the query it is immediately attached to - "files from A", 
but not to "files from B", so second dataset query lacks the namespace specification for the dataset B.

Here is how it can be corrected:

.. code-block:: sql

        with namespace="s"      
                (files from A - files from B)
        
And the outer "with" clause can be overridden by the inner clause:

.. code-block:: sql

        with namespace = "x"
                union (
                        files from A,
                        with namespace = "y"
                                join(
                                        files from B,
                                        files from C
                                ),
                        files from D
                )
                
In this example, datasets A and D will be assumed to be in the namespace "x", and datasets B and C - in
namespace "y".

Of course, explicit namespace specification overrides the one specified using "with":

.. code-block:: sql

        with namespace = "x"
                union (
                        files from A,
                        files from y:B,
                        files from C
                )
                

This will return union of datasets "x:A", "y:B" and "x:C".

Metadata Comparison
~~~~~~~~~~~~~~~~~~~

MQL supports the following comparison operators: <, <=, >, >=, ==, !=
The following operators can be used for string matching using regular expressions:
    
    * metatada_name ~ "pattern" - parameter matches the pattern
    * ~* - match ignoring case
    * !~ - no match
    * !~* - no match ignoring case
    
For example:

.. code-block:: sql

    files from dune:all where 
        DUNE_data.comment present 
        and DUNE_data.detector_config ~ "FELIX"


Array or Dictionary Elements Access
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the metadata parameter is an array or a dictionary, you can refer to its specific element using square brackets:

Assume the file metadata has the following parameters:

.. code-block:: json
    
    {
        "run_type":       "calibration",
        "trigger_mask":   [0,1,0,0,1],
        "trigger_bits":   
        {
            "muon":       1,
            "electron":   0
        },
        "modules":        ["a1", "a2", "a3"]
    }


Then:

    * ``trigger_bits["muon"] == 1`` - will match
    * ``trigger_bits["proton"] == 1`` - will not match
    * ``trigger_mask[3] == 0`` - will match

Also, you can use subscripts ``[any]`` as "any element of" and ``[all]`` as "all elements of" an array, but *not* dictionary:

    * ``trigger_bits[any] == 1`` - will match
    * ``trigger_bits[any] != 1`` - will match
    * ``trigger_bits[all] == 1`` - will not match
    * ``trigger_bits[all] != 1`` - will not match
    * ``trigger_bits[all] < 2`` - will match
    
You can also use ``in`` and ``not in`` to check if a value is contained in the array:

    * ``"a1" in modules`` - will match, equivalent to ``modules[any] = "a1"``
    * ``"xyz" not in modules`` - will match, equivalent to ``modules[all] != "xyz"`` or ``!(modules[any] = "xyz")``

Note that while `trigger_bits[all] != 1` will not match, `!(trigger_bits[all] == 1)` will match. In general, the following pairs of expressions are
equal:

    * ``array[all] != x`` and ``!(array[any] == x)``
    * ``array[any] != x`` and ``!(array[all] == x)``
    
To use size of the array in an expression, you len(): ``len(trigger_mask) > 2``

Ranges and Sets
~~~~~~~~~~~~~~~

Logical expressins can include ranges or sets of values. Here are some examples:

    * ``x in 3:5`` - if x is scalar, equivalent to ``(x >=3 and x <= 5)``
    * ``x in (3,4,5)`` - if x is scalar, equivalent to ``(x==3 or x==4 or x==5)``
    
Keep in mind that due to the way the underlying database works, queries with enumerated sets of allowed values work much faster than 
those with ranges.
So while the two expressions above are mathematically equivalent for integer numbers, second one will run much faster.

Sets and ranges can be expressed in terms of floating point numbers and strings:

    * ``application.version in "1.0":"2.3"``
    * ``pi in 3.131:3.152``
    * ``values[any] in 3:5``

Note that ``array[any] in low:high`` is `not` equivalent to ``(array[any] >= low and array[any] >= low)`` because former expression means:
"any element of the array is in the range" while the later one means "any element is greater or equal `low` and the same or another element 
of the array is less or equal `high`". For example, consider this metadata:

.. code-block:: json

    {
        "run_type":       "calibration",
        "sequence":  [1,1,2,3,5,8,13],
        "bits": [0,1,1,0,0]
    }

In this case,

    * ``sequence[any] in 6:7`` will not match because there is no single element in the array between 6 and 7,
    * ``(sequence[any] >= 6 and sequence[any] <= 7)`` will match because there are some elements below 7 and then some others above 6.
    
Similarly, the following expressions are not equivalent:

    * ``(bits[all] == 0 or bits[all] == 1)`` - is false for the metadata above
    * ``bits[all] in (0,1)`` - is true
    
Date and Time
~~~~~~~~~~~~~

Because of JSON limitations, date/time values are stored in metadata as integer or floating point timestamps - number
of seconds since the Epoch (January 1 1970 00:00:00 UTC). MQL offers 2 convenience functions to help the user include
date/time based conditions in the query.

datetime
--------

``datetime`` function will convert a text representation of date/time to the corresponding numeric timestamp value. The function
supports a subset of ISO 8601 date/time representation format:

.. code-block::

    YYYY-MM-DD[(T| )hh:mm:ss[.fff][(+|-)hh:mm]]

Here are some examples of supported date/time representation:


.. code-block::

    '2011-11-04'
    '2011-11-04T00:05:23'
    '2011-11-04 00:05:23.283'
    '2011-11-04 00:05:23.283+00:00'
    '2011-11-04T00:05:23+04:00’

If the time portion of the date/time representation is missing, the midnight (00:00:00) will be used. Default timezone is UTC.

This function can be used like this:

.. code-block:: sql

    files from namespace:dataset
        where core.timestamp > datetime("2011-11-04 00:05:23.283")
        
    files from namespace:dataset    # a safe string does not have to be quoted below
        where core.timestamp > datetime(2011-11-04T00:05:23)            

``datetime`` values can be used anywhere a floating point constant can appear, including range specifications:

.. code-block:: sql

    files from namespace:dataset
        where core.timestamp in datetime("2011-11-04 00:05:23.283"):datetime("2011-11-06 06:06:23")     # timestamp range

    files from namespace:dataset
        where core.timestamp in (       # timestamp set (not very useful)    
            datetime("2011-11-04 00:05:23.283"),
            datetime("2011-11-06 06:06:23")
        )   

date
----

``date`` function can be used to compare the timestamp stored in the database using 24 hours accuracy. The date can be specified as a string in format:

.. code-block::

    YYYY-MM-DD

The ``date`` function takes one or 2 parameters. First parameter is the date specification and the second optional parameter is the time zone specification
as a string in the format:

.. code-block::

    (+|-)hh:mm

Default time zone is UTC.

Examples:

.. code-block::

    date("2020-04-01")
    date(2020-04-01)                # safe string does not need to be quoted
    date(2020-04-01, -05:00)        # date with the timezone specification, unquoted safe strings
    
When a ``date`` value it compared to a numeric timestamp, first the numeric timestamp corresponding to the midnight of the specified date
in the specified (or UTC) timezone is calculated. Then the timestamp from the metadata is tested whether or not it is in the 24 hours interval
starting at the calculated timestamp.

``date`` function can be used in simple comparisons as well as value ranges:


.. code-block:: sql

    files from namespace:dataset
        where core.timestamp > date("2011-11-04")
        
    files from namespace:dataset
        where core.timestamp < date(2011-11-04, "-05:00")

    files from namespace:dataset
        where core.timestamp in date(2011-11-04, "-05:00") : date(2011-11-05, "+01:00")

In a range expression, if one of the range endpoints is a ``date``, the other endpoint must be a ``date`` too.

At present, ``date`` function can not be used in value set comparisons. For example, this will cause an error:

.. code-block:: sql

    files from namespace:dataset      
        where core.timestamp in (   # ERROR - not supported !
            date("2011-11-04"), 
            date("2011-11-06"), 
            date("2011-11-08")
        )

If you need to compare the date to a list of dates, use the logical ``or`` with simple comparisons:

.. code-block:: sql

    files from namespace:dataset
        where 
            core.timestamp = date("2011-11-04")
            or core.timestamp = date("2011-11-06")
            or core.timestamp = date("2011-11-08")



Segmenting Query Results
~~~~~~~~~~~~~~~~~~~~~~~~

If you want to see only a portion of the resulting file set, add ``limit <n>`` to your query:

.. code-block:: sql

    files from dune:all where 
        DUNE_data.detector_config.list present 
        limit 100

Limit clause can be added to results of any query:
        
.. code-block:: sql

    union (
        files from dune:all where 
            DUNE_data.detector_config.list present 
            limit 100
        ,
        files from dune:mc where 
            len(core.events) > 10 
    ) limit 200
        
        
Another way of limiting query results is to use built-in "sample" query:

.. code-block:: sql

    filter sample(0.1) (
        files from dune:all where 
            DUNE_data.detector_config.list present 
            limit 10000
    )
        
The "sample" filter returns the given fraction of the input query results. In this case, the results will be limited to 1000 (=10000*0.1) files.

To skip some files from the beginning of the file set, use ``skip <n>`` clause:

.. code-block:: sql

    files from dune:all where 
        DUNE_data.detector_config.list present 
        skip 100 
        limit 100
 
``limit`` and ``skip`` are applied independently in the order as they are written. For example, the query

.. code-block:: sql

    files from dune:all where 
        DUNE_data.detector_config.list present 
        skip 100 
        limit 1000
        skip 10
        skip 5
        limit 50
        
is interpreted like this:

.. code-block:: sql

    (
        (
            (
                (
                    (
                        files from dune:all where 
                            DUNE_data.detector_config.list present 
                    ) skip 100
                ) limit 1000
            ) skip 10
        ) skip 5
    ) limit 50

and it is equivalent to:

.. code-block:: 

    files from dune:all where 
        DUNE_data.detector_config.list present 
        skip 115 
        limit 50
        
Another illustration of the fact that ``skip`` and ``limit`` caluses are applied sequentially in the order they are written is that
while this query may return up to 50 files, depending on the size of the dataset:

.. code-block:: 

    files from dune:all 
        skip 50                     # skip first 50 files
        limit 50                    # return next 50 (up to 50 to be exact)

if the order of ``skip`` and ``limit`` is reversed, the result of the query is guaranteed to be *empty*:


.. code-block:: 

    files from dune:all 
        limit 50                    # take only first 50 files
        skip 50                     # then skip all of them -> empty result





.. _ordering:

Query Results Ordering
~~~~~~~~~~~~~~~~~~~~~~

Because sorting query results takes additional time and is not always necessary, 
by deafult, MetaCat does not sort the file set returned by the query in any particular order, and therefore, can not guarantee
that the same query will always return results in the same order. However, if necessary, the user can request that the
query results order is deterministic. To do that, add keyword ``ordered`` to any query:

.. code-block::
    
    # order of resulting file set is not guaranteed:
    files from dc4:dc4 
        where 12345 in core.runs
    
    # order of resulting file is guaranteed:
    files from dc4:dc4 
        where 12345 in core.runs 
        ordered

Ordered query is guaranteed to return entries in the same order as long as the query produces the same set of results.

Another case when the query results order is guaranteed is when ``skip`` is used. In this case, MQL implicitly
makes the underlying query ordered. For example:

.. code-block::
    
    files from dc4:dc4 
        where 12345 in core.runs
        skip 100 
    
is equivalent to:

.. code-block::
    
    (
        (
            files from dc4:dc4 
                where 12345 in core.runs
        ) ordered
    ) skip 100 

This feature makes it easy to split large sets of results into smaller parts in a consistent manner. For example, one can use the following 3 queries
to process a 15000 file dataset in 5000 files chunks:

.. code-block::

    files from scope:Dataset15K skip 0     limit 5000
    files from scope:Dataset15K skip 5000  limit 5000
    files from scope:Dataset15K skip 10000 limit 5000

Of course this will work only if no files are added to or removed from the dataset between the queries.

Dataset Queries
~~~~~~~~~~~~~~~

Simplest dataset query looks like this:

.. code-block:: sql

    datasets matching test:*
    
This query will return all the datasets from the "test" namespace.

To select datasets by metadata:

.. code-block:: sql

    datasets matching test:*
        having data.type="mc" and detector.id="near"
        
Dataset query can combine multiple dataset selections separated with comma:

.. code-block:: sql

        datasets mathcing prod:XYZ%_3 having data.type=mc,
                matching mc:XYZ%_4
    
To add immediate dataset children:

.. code-block:: sql

    datasets matching test:*
        with subsets
        having data.type="mc"

This will find all the datasets mathiching the namespace:name pattern, add their immediate children and then filter the resulting set of
datasets by their metadata.

To get all subsets, recursively:

.. code-block:: sql

    datasets test:a with subsets recursively,
            test:c with subsets,
            matching test:x*

Dataset name patterns in the above examples use POSIX pattern syntax. They can include eiher '*' to match any substring or '?'
to match a single character. SQL style can be used too where '%' will match a substring and '_' will match any single character.

There is also a way to use regular expressions. To do that, the `regexp` keyword must be included after the `matching` keyword
and the regular expression has to be taken into quotes:

.. code-block:: sql

        datasets mathcing regexp prod:"XYZ_3[a-z0-9]+" having type="mc" and detector="near",
                matching regexp mc:"XYZ.*_4"

Combining File and Dataset Metadata Filtering
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

(this is not fully implemented yet)

Dataset and file metadata filtering can be mixed together:

.. code-block:: sql

    files from 
        datasets matching production:% 
            having data.type="mc" and detector.id="near"    # dataset selection
        where beam.status="on" and reco.version > "3.0"     # files selection
        
    
.. _named_queries:

Named Query Search
~~~~~~~~~~~~~~~~~~

MetaCat allows the user to save a query under a namespace/name and then reuse the query as part of
another MQL query. Currently only file queries can be saved.

Along with a name and a description, a named query can have its own set of metadata attributes.
MetaCat provides a capability to search for named queries by their name pattern, description,
creator and metadata attributes. A subset of MQL is used to search for a named query.

Here are some examples of named query search queries:

.. code-block:: sql

    queries matching my_namespace:favorite_*
    queries matching regexp my_namespace:"prod_202[0-3]"

To include the query metadata into the search criteria, add `where` clause:

.. code-block:: sql

    queries matching my_namespace:favorite_*
        where file.quality > 1 and file.type = "hdf5"


        
