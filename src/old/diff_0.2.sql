create table users
(
    username    text    primary key,
    name        text,
    email       text,
    flags       text
);

create table categories
(
    path        text    primary key,
    owner       text    references  users(username),
    restricted  boolean default 'false'
);

create table parameter_definitions
(
    category    text    references categories(path),
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

