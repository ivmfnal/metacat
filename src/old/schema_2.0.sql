create table users
(
    username    text    primary key,
    name        text,
    email       text,
    flags       text
);

create table authenticators
(
    username    text    references users(username),
    type        text
        constraint authenticator_types check ( 
            type in ('x509','password','ssh')
            ),
    secrets      text[],
    primary key(username, type)
);

create table namespaces
(
	name	text	primary key,
	owner	text    references  users(username)
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
    parent_id   text references files(id),
    child_id    text references files(id),
    parent_sequence    int,
    primary key (parent_id, child_id, parent_sequence)
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

create table parameter_categories
(
    path        text    primary key,
    owner       text    references  users(username),
    restricted  boolean default 'false'
);

create table parameter_definitions
(
    category    text    references parameter_categories(path),
    name        text,
    type        text
        constraint attribute_types check ( 
            type in ('int','double','text','boolean',
                    'int array','double array','text array','boolean array')
            ),
    int_values      bigint[],
    int_min         bigint,
    int_max         bigint,
    double_values   double precision[],
    double_min      double precision,
    double_max      double precision,
    text_values     text[],
    text_pattern    text,
    primary key(category, name)
);

    


