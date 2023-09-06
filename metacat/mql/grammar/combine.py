from .common import Common
from .file_query import FileQuery
from .dataset_query import DatasetQuery
from .query_query import QueryQuery

MQL_Grammar = Common + FileQuery + DatasetQuery + QueryQuery