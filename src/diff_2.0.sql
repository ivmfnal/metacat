create table users
(
    username    text    primary key,
    name        text,
    email       text,
    flags       text    default ''
);

insert into users(username, name, email) values (
    'ivm','Igor Mandrichenko','ivm@fnal.gov'
);

alter table namespaces add constraint fk_namespaces_users foreign key (owner) references users(username);

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

