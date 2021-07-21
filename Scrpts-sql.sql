create database inss;
use inss;

create table dim_especie(
id int not null auto_increment,
especie varchar(1000) not null, 
primary key(id));

create table dim_sexo(
id int not null auto_increment,
sexo varchar(1000) not null, 
primary key(id));

create table dim_uf(
id int not null auto_increment,
uf varchar(1000) not null, 
primary key(id));

create table dim_despacho(
id int not null auto_increment,
despacho varchar(1000) not null, 
primary key(id));

create table dim_clientela(
id int not null auto_increment,
clientela varchar(1000) not null, 
primary key(id));

create table fato_qtd_rendamensalinicial(
id int not null auto_increment,
data_concessao varchar(1000) not null,
quantidade numeric not null,
id_especie int not null,
id_despacho int not null,
data_nascimento varchar(1000) not null,
id_sexo int not null,
id_clientela int not null,
municipio varchar(1000) not null,
vinculo_dependentes varchar(1000) not null,
forma_filiacao varchar(1000) not null,
id_uf int not null,
primary key(id));

alter table fato_qtd_rendamensalinicial add foreign key(id_especie) references dim_especie(id);
alter table fato_qtd_rendamensalinicial add foreign key(id_despacho) references dim_despacho(id);
alter table fato_qtd_rendamensalinicial add foreign key(id_sexo) references dim_sexo(id);
alter table fato_qtd_rendamensalinicial add foreign key(id_clientela) references dim_clientela(id);
alter table fato_qtd_rendamensalinicial add foreign key(id_uf) references dim_uf(id);
