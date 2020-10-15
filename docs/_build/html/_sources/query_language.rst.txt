MQL - Metadata Query Language
=============================

Introduction
~~~~~~~~~~~~
One of the functions of the Metadata Database is to produce list of files matching a set of crieria specidied
by the user. The product has its own simple language to write these queries in called MQL (pronpounced: MEE-quel,
like "sequel", but with M). MQL is a language to describe queries against the metadata database.
A query produces a set of files. The order of files in the returned set is not guaranteed and can not be
relied on. 

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

Also, you can use wildcards in the dataset name (but not in the scope name). If the dataset name is in quotes,
it is interpreted as an SQL wildcard.

.. code-block:: sql

        files from MyScope:"MC%", AnotherScope:MC

Note that you have to use database wildcard notation where '%' matches any string, including empty string, and '_' matches any single
character.

In particular, if you want to select all files from all known datasets, you can do this:

.. code-block:: sql

        files from "%"
                where run=1234



Metadata Filtering
------------------

Results of any query can be filtered by adding some metadata criteria expression, called *meta-filter*:

.. code-block:: sql

        files from MyScope:MyDataset
                where x > 0.5
                
This will return all the files in the dataset, which have a floating point metadata field named "x" with value greater than 0.5. A meta-filter can be more complicated:

.. code-block:: sql

        files from MyScope:MyDataset
                where x > 0.5 and x < 1.5 
                        and run = 123 
                        and ( type="MC" or type="Data" )
                        
Generally, all white space is ignored in MQL.
                
Combining Queries
-----------------

Queries can be combined using boolean operations *union*, *join*, and subtraction to produce new queries:

.. code-block:: sql

        union(
                files from MC:Cosmics
                        where p > 0.5 and p < 1.5 
                files from MC:Beam where e = 10
        )
        
This query will return files from both datasets. Even if the individual queries happen to produce overallping
sets of files, each file will appear only *once* in the results of the *union* query.

Queries can be *joined* to procude the intersection of the results of individual queries:

.. code-block:: sql

        join(
                files from MC:All
                        where p > 0.5 and p < 1.5 
                files from MC:All
                        where e = 10
        )
        
Of course this is equivalent to:

.. code-block:: sql

        files from MC:All
                where p > 0.5 and p < 1.5 and e = 10
        
Queries can be subtracted from each other, which means the resulting set will be boolean subtraction of second query
result set from the first:

.. code-block:: sql

        files from MC:Beam where e1 > 10 - files from MC:Exotics
        
Although is it not necessary in this example, you can use parethesis and white space to make the query more readable:

.. code-block:: sql

        (files from MC:Beam where e1 > 10) 
        - (files from MC:Exotics where type = "abcd")

Also, you can use square and curly brackets as an alternative to using explicit words "union" and "join" respectively.
The following two queries are equivalent:

.. code-block:: sql

        [
                files from s:A,
                {
                        files from s:B,
                        files from s:C
                }
        ]

        union (
                files from s:A,
                join(
                        files from s:B,
                        files from s:C
                )
        )
        
External Filters
----------------

The Meatadata Database Query Engine lets the user add custom Python code to be used as a more complicated
operations on the file sets. They in the Query Language, they are invoked using "filter" keyword:

.. code-block:: sql

        filter sample(0.5)( files from s:A )
        
Here, *filter* the the keyword, *sample* is the name of the Python function to be used to filter the results
of the argument query (simple "files from s:A" query in this case). As you can see, you can pass some
parameters to the function (the number 0.5).

A filter can accept multiple parameters and/or queries:

.. code-block:: sql

        filter process(0.5, 1, 3.1415)
                ( files from s:A, files from s:B - files from s:D )

The user supplied function looks like ths:

.. code-block:: Python

        def process(params, inputs):
                # ...
                return iterable
                
The *params* argument will receive the pist of parameters and the *inputs* will get the list of
input file sets. The function is supposed to return a single iterable (a list, a generator, etc.) as the
output file set.


Common Namesaces
----------------

Typically (but not necessarily), all the datasets mentioned in a query refer to the same namespace.
You can avoid repeting the same namespace using "with" clause. The following are equivalent:

.. code-block:: sql

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


Dataset Queries
~~~~~~~~~~~~~~~

Simplest dataset query looks like this:

.. code-block:: sql

    datasets test:"%"
    
This query will return all the datasets from the "test" namespace.

To add some metadata filtering, add "having" clouse to the query:

.. code-block:: sql

    datasets test:"%"
        having type="mc" and detector="near"
        
Dataset queries can be combined in the same way as the file queries:

.. code-block:: sql

    union (
        datasets prod:"XYZ%_3",
        datasets mc:"XYZ%_4"
    )
    
"union", "join" with their brackets synonims and subtraction are working in the same way as for file queries.


Combining File and Dataset Metadata Filtering
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

(this is not fully implemented yet)

Dataset and file metadata filtering can be mixed together:

.. code-block:: sql

    files from mc:"%" having type="nc" and detector="near"
        where beam="on" and version>3
        
    



        
