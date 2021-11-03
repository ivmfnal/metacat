create table namespaces
(
 	name	text	primary key,
 	owner	text
);

insert into namespaces(name, owner) (
	select distinct namespace, owner from
	(
        	select namespace, 'ivm' as owner        from files
        	union
        	select namespace, 'ivm' as owner        from datasets
        	union
        	select namespace, 'ivm' as owner        from queries
	) as qq	
);

alter table files add constraint fk_files_namespaces foreign key (namespace) references namespaces(name);
alter table datasets add constraint fk_dataset_namespaces foreign key (namespace) references namespaces(name);
alter table datasets add column frozen		boolean default 'false';
alter table datasets add column monotonic		boolean default 'false';
alter table queries add constraint fk_queries_namespaces foreign key (namespace) references namespaces(name);
 
