from .dbobjects2 import (
    DBUser, DBDataset, DBFile, DBRole, DBNamespace, DBFileSet, DBNamedQuery
)

from .common import (
    AlreadyExistsError, NotFoundError, IntegrityError, MetaValidationError, DatasetCircularDependencyDetected,
    parse_name, alias, make_list_if_short
)

from .param_category import DBParamCategory

import os.path as os_path

here = os_path.dirname(__file__)

drop_tables_file = os_path.join(here, "drop_all.sql")
drop_tables_sql = open(drop_tables_file, "r").read() if os_path.isfile(drop_tables_file) else None

create_schema_file = os_path.join(here, "schema.sql")
create_schema_sql = open(create_schema_file, "r").read() if os_path.isfile(create_schema_file) else None
del os_path, create_schema_file, drop_tables_file, here