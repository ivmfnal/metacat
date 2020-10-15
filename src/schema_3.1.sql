create table users
(
    username    text    primary key,
    name        text,
    email       text,
    flags       text    default ''
);

create table roles
(
    name        text    primary key,
    description text,
    users       text[]  default '{}'::text[]
);

--insert into roles(name, description) values ('admin', 'Administrator');

create table authenticators
(
    username    text    references users(username) on delete cascade,
    type        text
        constraint authenticator_types check ( 
            type in ('x509','password','ssh')
            ),
    secrets      text[],
    primary key(username, type)
);

create table namespaces
(
	name                text        primary key,
	owner               text        references  roles(name),
    creator        text references users(username),
    created_timestamp   timestamp with time zone        default now()
);

create table files
(
    id          text    primary key,
    namespace   text 	references namespaces(name),
    name        text,
    metadata    jsonb,
    creator text references users(username),
    size        bigint,
    created_timestamp   timestamp with time zone    default now()
);

create unique index file_names_unique on files(namespace, name);
create index files_meta_index on files using gin (metadata);

create table parent_child
(
    parent_id   text references files(id),
    child_id    text references files(id),
    primary key (parent_id, child_id)
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
    foreign key (parent_namespace, parent_name) references datasets(namespace, name),
    metadata    jsonb,
    required_metadata   text[],
    creator        text references users(username),
    created_timestamp   timestamp with time zone     default now(),
    expiration          timestamp with time zone,
    description         text
);

create index datasets_meta_index on datasets using gin (metadata);

create table files_datasets
(
    file_id                 text    references files on delete cascade,
    dataset_namespace       text,
    dataset_name            text,
    primary key(dataset_namespace, dataset_name, file_id)
);       

create index files_datasets_file_id on files_datasets(file_id);

create table queries
(
    namespace       text references namespaces(name),
    name            text,
    parameters      text[],
    source      text,
    primary key(namespace, name),
    creator             text references users(username),
    created_timestamp   timestamp with time zone     default now()
);

create table parameter_categories
(
    path        text    primary key,
    owner       text    references  roles(name),
    restricted  boolean default 'false',
    creator             text references users(username),
    created_timestamp   timestamp with time zone     default now(),
    definitions         jsonb
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
    bollean_value   boolean,
    required        boolean,
    creator             text references users(username),
    created_timestamp   timestamp with time zone        default now(),
    primary key(category, name)
);

    


