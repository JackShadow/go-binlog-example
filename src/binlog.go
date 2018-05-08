package main

import (
	"github.com/siddontang/go-mysql/canal"
	"fmt"
	"runtime/debug"
)

type binlogHandler struct {
	canal.DummyEventHandler
	BinlogParser
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()
	var n int
	var k int
	switch e.Action {
	case canal.DeleteAction:
		return nil
	case canal.UpdateAction:
		n = 1
		k = 2
	case canal.InsertAction:
		n = 0
		k = 1
	}

	for i := n; i < len(e.Rows); i += k {

		key := e.Table.Schema + "." + e.Table.Name

		switch key {
		case User{}.SchemaName() + "." + User{}.TableName():
			user := User{}
			h.GetBinLogData(&user, e, i)

			if e.Action == canal.UpdateAction {
				oldUser := User{}
				h.GetBinLogData(&oldUser, e, i-1)
				fmt.Printf("User %d name changed from %s to %s\n", user.Id, oldUser.Name, user.Name, )
			} else {
				fmt.Printf("User %d is created with name %s\n", user.Id, user.Name, )
			}
		}

	}
	return nil
}

func (h *binlogHandler) String() string {
	return "binlogHandler"
}

func binlogListener() {
	c, err := getDefaultCanal()
	if err == nil {
		coords, err := c.GetMasterPos()
		if err == nil {
			c.SetEventHandler(&binlogHandler{})
			c.RunFrom(coords)
		}
	}
}

func getDefaultCanal() (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", "mariadb", 3307)
	cfg.User = "root"
	cfg.Password = "root"
	cfg.Flavor = "mysql"

	cfg.Dump.ExecutionPath = ""

	return canal.NewCanal(cfg)
}
