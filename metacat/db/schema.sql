
create table users
(
    username    text    primary key,
    name        text,
    email       text,
    flags       text    default '',
    auth_info   jsonb   default '{}',
    auid        text                        -- anonymized user identificator
);

create table authenticators
(
    username    text references users(username),
    type        text,
    issuer      text,
    primary key(username, type, issuer),
    user_info   jsonb   default '{}'
);

create table roles
(
    name        text    primary key,
    parent_role text    references roles(name),
    description text
);

create table users_roles
(
    username    text    references users(username),
    role_name        text    references roles(name),
    primary key(username, role_name)
);

create table namespaces
(
	name                text        primary key,
    check( name != ''),
    
    description         text,

	owner_user          text        references  users(username),
	owner_role          text        references  roles(name),
    check ( (owner_user is null ) != (owner_role is null) ),
    
    creator        text references users(username),
    created_timestamp   timestamp with time zone        default now(),
    file_count  bigint          default 0
);

create table files
(
    id          text    primary key,
    namespace   text 	references namespaces(name),
    name        text,
    metadata    jsonb   default '{}',
    creator     text references users(username),
    size        bigint,
    checksums   jsonb   default '{}',
    created_timestamp   timestamp with time zone    default now(),
    updated_by  text references users(username),
    updated_timestamp   timestamp with time zone    default now(),
    retired     boolean default false,
    retired_timestamp   timestamp with time zone,
    retired_by  text references users(username)
);

create unique index file_names_unique on files(namespace, name) include (id);
create index files_meta_path_ops_index on files using gin (metadata jsonb_path_ops);
create index files_meta_index on files using gin (metadata);

create index files_creator on files(creator);
create index files_created_timestamp on files(created_timestamp);
create index files_size on files(size);
create index files_name on files(name) include (namespace, id);

create table parent_child
(
    parent_id   text references files(id),
    child_id    text references files(id),
    primary key (parent_id, child_id)
);

create index parent_child_child on parent_child(child_id, parent_id);

create view file_provenance as
    select f.id, 
        array(select parent_id from parent_child pc1 where pc1.child_id=f.id) as parents, 
        array(select child_id from parent_child pc2 where pc2.parent_id=f.id) as children
    from files f
;    

create view files_with_provenance as
    select f.*, r.children, r.parents
    from files f, file_provenance r
    where f.id = r.id
;

create table datasets
(
    namespace           text references namespaces(name),
    name                text,
    primary key (namespace, name),

    frozen		        boolean default 'false',
    monotonic		    boolean default 'false',
    metadata    jsonb   default '{}',
    required_metadata   text[],
    creator             text references users(username),
    created_timestamp   timestamp with time zone     default now(),
    expiration          timestamp with time zone,
    description         text,
    file_metadata_requirements  jsonb   default '{}'::jsonb,
    file_count  bigint          default 0,
    updated_timestamp   timestamp with time zone,
    updated_by          text references users(username)
);

create index datasets_meta_index on datasets using gin (metadata);
create index datasets_meta_path_ops_index on datasets using gin (metadata jsonb_path_ops);
create index datasets_spec on datasets( (namespace || ':' || name ));

create table datasets_parent_child
(
    parent_namespace    text,
    parent_name         text,
    child_namespace     text,
    child_name          text,
    foreign key (parent_namespace, parent_name) references datasets(namespace, name) on delete cascade,
    foreign key (child_namespace, child_name) references datasets(namespace, name) on delete cascade,
    primary key (parent_namespace, parent_name, child_namespace, child_name)
);

create index datasets_parent_child_child on datasets_parent_child(child_namespace, child_name);
create index datasets_parent_child_child_spec on datasets_parent_child( (child_namespace || ':' || child_name ));
create index datasets_parent_child_parent_spec on datasets_parent_child( (parent_namespace || ':' || parent_name ));

create table files_datasets
(
    file_id                 text    references files on delete cascade,
    dataset_namespace       text,
    dataset_name            text,
    foreign key(dataset_namespace, dataset_name) references datasets(namespace, name) on delete cascade,
    primary key(dataset_namespace, dataset_name, file_id)
);       

create index files_datasets_file_id on files_datasets(file_id);

create table queries
(
    namespace       text references namespaces(name),
    name            text,
    primary key(namespace, name),
    parameters      text[],
    source          text,
    creator         text references users(username),
    created_timestamp   timestamp with time zone     default now(),
    description     text,
    metadata        jsonb default '{}'
);

create table parameter_categories
(
    path        text    primary key,
    
    owner_user          text        references  users(username),
    owner_role          text        references  roles(name),
    
    check ( (owner_user is null ) != (owner_role is null) ),
    
    restricted  boolean default 'false',
    description         text,
    creator             text references users(username),
    created_timestamp   timestamp with time zone     default now(),
    definitions         jsonb
);




