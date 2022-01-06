from .common import Common
from .file_query import FileQuery
from .dataset_query import DatasetQuery

MQL_Grammar = Common + FileQuery + DatasetQuery