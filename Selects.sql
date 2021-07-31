create table teste(
id int not null auto_increment,
teste varchar(1000) not null, 
primary key(id));

INSERT INTO teste (teste) VALUES ('Fernando')

select * from teste;
select * from dim_clientela;
select * from dim_despacho;
select * from dim_especie;
select * from dim_sexo;
select * from dim_uf;
select * from dim_faixaetaria;
select * from fato_qtd_rendamensalinicial;