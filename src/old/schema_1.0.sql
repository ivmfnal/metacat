create table namespaces
(
	name	text	primary key,
	owner	text
);

create table files
(
    id          text    primary key,
    namespace   text 	references namespaces(name),
    name        text
);

create unique index file_names_unique on files(namespace, name);

create table parent_child
(
    parent_id    text,
    child_id    text,
    sequence    int,
    primary key (parent_id, child_id, sequence)
);

create index parent_child_child on parent_child(child_id);

create table datasets
(
    namespace           text references namespaces(name),
    name                text,
    parent_namespace    text,
    parent_name         text,
    frozen		boolean default 'false',
    monotonic		boolean default 'false',
    primary key (namespace, name),
    foreign key (parent_namespace, parent_name) references datasets(namespace, name) 
);

create table files_datasets
(
    file_id                 text    references files,
    dataset_namespace       text,
    dataset_name            text,
    primary key(file_id, dataset_namespace, dataset_name),
    foreign key(dataset_namespace, dataset_name) references datasets(namespace, name)
);       

create table file_attributes
(
    file_id         text    references files,
    name            text,
    int_value       bigint,
    float_value     double precision,
    string_value    text,
    bool_value      boolean,
    int_array       bigint[],
    float_array     double precision[],
    string_array    text[],
    bool_array      boolean[],
    primary key(file_id, name)
);

create index file_attr_int on file_attributes(name, int_value, file_id);
create index file_attr_float on file_attributes(name, float_value, file_id);
create index file_attr_string on file_attributes(name, string_value, file_id);
create index file_attr_bool on file_attributes(name, bool_value, file_id);

create table dataset_attributes
(
    dataset_namespace      text,
    dataset_name    text,
    name            text,
    int_value       bigint,
    float_value     double precision,
    string_value    text,
    bool_value      boolean,
    int_array       bigint[],
    float_array     double precision[],
    string_array    text[],
    bool_array      boolean[],
    primary key(dataset_namespace, dataset_name, name),
    foreign key (dataset_namespace, dataset_name) references datasets(namespace, name)
);

create index dataset_attr_int on dataset_attributes(name, int_value, dataset_namespace, dataset_name);
create index dataset_attr_float on dataset_attributes(name, float_value, dataset_namespace, dataset_name);
create index dataset_attr_string on dataset_attributes(name, string_value, dataset_namespace, dataset_name);
create index dataset_attr_bool on dataset_attributes(name, bool_value, dataset_namespace, dataset_name);

create table queries
(
    namespace       text references namespaces(name),
    name            text,
    parameters      text[],
    source      text,
    primary key(namespace, name)
);


