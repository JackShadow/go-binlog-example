### Golang binglog example project

In binlog.go change with your connection data
```
cfg.Addr = fmt.Sprintf("%s:%d", "127.0.0.1", 3306) //host,port
cfg.User = "root"
cfg.Password = "root"
```

Create database with sql
```
create table Test.User
(
  id int auto_increment primary key,
  name varchar(40) null,
  status enum("active","deleted") DEFAULT "active",
  created timestamp default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP
)
  engine=InnoDB;
```

```
go get
go build src/*
./binlog
```
than execute sql
```
INSERT Into Test.User (`id`,`name`) VALUE (1,"Jack");
UPDATE Test.User SET name="Jonh" WHERE id=1;

```

and you will see
```
User 1 is created with name Jack
User 1 is updated from name Jack to name Jonh
```