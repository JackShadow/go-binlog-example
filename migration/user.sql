create table Test.User
(
  id int auto_increment primary key,
  name varchar(40) null,
  status enum("active","deleted") DEFAULT "active",
  created timestamp default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP
)
  engine=InnoDB;


INSERT Into Test.User (`id`,`name`) VALUE (1,"Jack");
UPDATE Test.User SET name="Jonh" WHERE id=1;
DELETE FROM Test.User WHERE id=1;