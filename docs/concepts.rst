MetaCat Concepts
================

MetaCat is a general purpose metadata catalog. It has 4 major functions:

1. Store metadata associated with a *file*

2. Provide a mechanism to retrieve the metadata associated with a file

3. Efficiently query the metadata database to find files matching the list of crieria expressed in terms of file metadata

4. Provide a mechanism to seamlessly and efficiently integrate metadata stored in external sources to the query results and use it select files based on their metadata values


File
----
MetaCat is a purely metadata database. Replica management is outside of MetaCat scope. That is why in MetaCat, *file* is
rather abstract object. MetaCat "file" is anything wich has the following attributes:

* Unique text *file id*
* Unique *Name* within a *Namespace*
* *Metadata* dictionary
* File *provenance* information - list of file *parents* and *clildren*

MetaCat also stores the following file attributes, but they are not used by MetaCat itself, and therefore, depending on the
use case, they do not necessarily have to have some meaningful values:

* File creator username
* Creation time
* File size
* One or more checksums

File also can be a memeber of one or more *Datasets*

Dataset
-------
In MetaCat, *Dataset* is a relatively static collection of files. "Relatively static" means that files are added to and removed from
a dataset explicitly. There is no such thing as a "dynamic" dataset, which automatically contains files matching certain criteria.
MetaCat Dataset has the following attributes:

* Unique *Name* within a *Namespace*
* *Metadata* dictionary

A Dataset can have zero or more subsets. MetaCat enforces non-circular relationship between data datasets, i.e. it is impossible
to add a dataset as its own direct ot indirect subset.

Dataset also has the following attributes, not used by MetaCat:

* Creator username
* Creation time

Dataset can define metadata restrictions. They can be used to enforce certain requirements for files added to the dataset. The rules can be defined to:

* Require certain metadata fields to be present in the file metadata
* Define acceptable ranges, enumeration or patterns for parameter values

Query
-----

MetaCat query is an algorithm to select files based on the set of criteria defined by the user. Result of a query execution is a *file set*.
File set is an unordered collection of files, which match given set of criteria at the time when the query is executed.
Because the contents of the database is dynamic and can change at any time, the same query is *not* guaranteed to always return the same results 
next time it is executed.

Currently, MetaCat does not have a mechanism to specify the order of the resulting file set. Therefore, even if the set of files returned by the 
query is the same, MetaCat does not guarantee that they are returned in the same order.

MetaCat queries are written in Metadata Query Language (MQL). Fundamental concept behind MQL is that it provides a mechanism to
build a complicated query from simpler queries. The file sets produced by simple queries are transformed into results of more complicated
queries as the query is executed.

Most of MQL queries are translated internally into SQL. This allows most of the queries to be executed by the database engine, which is supposed
to be able to do that efficiently. The only exception is when an *external data filter* is used in the query.
In this case, MetaCat translates portions of the query into SQL as much as possible and the rest of the query.

