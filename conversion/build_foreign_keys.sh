#!/bin/bash

source ./config.sh

$OUT_DB_PSQL << _EOF_

alter table parent_child add foreign key (parent_id) references files(id);
alter table parent_child add foreign key (child_id) references files(id);

alter table files add foreign key(creator)      references users(username);
alter table files add foreign key(retired_by)   references users(username);
alter table files add foreign key(updated_by)   references users(username);
alter table files add foreign key(namespace)    references namespaces(name);

alter table files_datasets  add foreign key (dataset_namespace, dataset_name) 
                                references datasets(namespace, name) 
                                on delete cascade,
                            add foreign key (file_id) 
                                references files(id) 
                                on delete cascade;

alter table parameter_categories add foreign key (owner_user) references users(username);
alter table parameter_categories add foreign key (owner_role) references roles(name);
alter table parameter_categories add foreign key (creator) references users(username);

alter table namespaces add foreign key (owner_user) references users(username);
alter table namespaces add foreign key (owner_role) references roles(name);
alter table namespaces add foreign key (creator) references users(username);

alter table datasets add foreign key(namespace) references namespaces(name),
                     add foreign key(creator) references users(username)
                     ;

_EOF_

