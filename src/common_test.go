package main

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/schema"
	"testing"
	"time"
)

func TestCommonHandler_GetBinLogDataGetBinLogData_Insert(t *testing.T) {

	model := binlogTestStruct{}
	handler := BinlogParser{}
	rows := make([][]interface{}, 1)
	insertRows := make([]interface{}, 8)
	insertRows[0] = 1
	insertRows[1] = 1
	insertRows[2] = 1.123
	insertRows[3] = int64(1)
	insertRows[4] = "test text"
	insertRows[5] = "2018-02-16 14:28:09"
	insertRows[6] = nil
	insertRows[7] = []byte("test text")
	rows[0] = insertRows

	columns := make([]schema.TableColumn, 8)

	columns[0] = schema.TableColumn{Name: "int", Type: schema.TYPE_NUMBER}
	columns[1] = schema.TableColumn{Name: "bool", Type: schema.TYPE_NUMBER}
	columns[2] = schema.TableColumn{Name: "float", Type: schema.TYPE_FLOAT}
	columns[3] = schema.TableColumn{Name: "enum", Type: schema.TYPE_ENUM, EnumValues: []string{"Active", "Deleted"},}
	columns[4] = schema.TableColumn{Name: "string", Type: schema.TYPE_STRING}
	columns[5] = schema.TableColumn{Name: "time", Type: schema.TYPE_TIMESTAMP}
	columns[6] = schema.TableColumn{Name: "enum_null", Type: schema.TYPE_ENUM, EnumValues: []string{"Active", "Deleted"},}
	columns[7] = schema.TableColumn{Name: "byte_text", Type: schema.TYPE_STRING}
	table := schema.Table{Schema: "test", Name: "test", Columns: columns}

	e := canal.RowsEvent{Table: &table, Action: canal.InsertAction, Rows: rows}
	handler.GetBinLogData(&model, &e, 0)

	if model.Int != insertRows[0] {
		t.Errorf("Int value did not update.")
	}
	if model.Bool != true {
		t.Errorf("Bool value did not update.")
	}
	if model.Float != insertRows[2] {
		t.Errorf("Float value did not update.")
	}
	if model.Enum != "Active" {
		t.Errorf("Enum value did not update.")
	}
	if model.String != insertRows[4] {
		t.Errorf("String value did not update.")
	}
	timeValue, _ := time.Parse("2006-01-02 15:04:05", insertRows[5].(string))
	if model.Time.Unix() != timeValue.Unix() {
		t.Errorf("DateTime value did not update.")
	}
	if model.EnumNull != "" {
		t.Errorf("Null enum was not parsed as string.")
	}

}

func TestCommonHandler_GetBinLogDataGetBinLogData_Update(t *testing.T) {

	model := binlogTestStruct{}
	handler := BinlogParser{}
	rows := make([][]interface{}, 2)
	insertRows := make([]interface{}, 8)
	insertRows[0] = 1
	insertRows[1] = 0
	insertRows[2] = 1.123
	insertRows[3] = int64(1)
	insertRows[4] = "test text"
	insertRows[5] = "2018-02-16 14:28:09"
	insertRows[6] = int64(1)
	insertRows[7] = []byte("test text")
	rows[0] = insertRows
	updateRows := make([]interface{}, 8)
	updateRows[0] = 3
	updateRows[1] = 1
	updateRows[2] = 2.234
	updateRows[3] = int64(2)
	updateRows[4] = "test2 text2"
	updateRows[5] = "2018-02-16 15:28:09"
	updateRows[6] = nil
	updateRows[7] = []byte("test2 text2")
	rows[1] = updateRows
	columns := make([]schema.TableColumn, 8)

	columns[0] = schema.TableColumn{Name: "int", Type: schema.TYPE_NUMBER}
	columns[1] = schema.TableColumn{Name: "bool", Type: schema.TYPE_NUMBER}
	columns[2] = schema.TableColumn{Name: "float", Type: schema.TYPE_FLOAT}
	columns[3] = schema.TableColumn{Name: "enum", Type: schema.TYPE_ENUM, EnumValues: []string{"Active", "Deleted"},}
	columns[4] = schema.TableColumn{Name: "string", Type: schema.TYPE_STRING}
	columns[5] = schema.TableColumn{Name: "time", Type: schema.TYPE_TIMESTAMP}
	columns[6] = schema.TableColumn{Name: "enum_null", Type: schema.TYPE_ENUM, EnumValues: []string{"Active", "Deleted"},}
	columns[7] = schema.TableColumn{Name: "byte_text", Type: schema.TYPE_STRING}
	table := schema.Table{Schema: "test", Name: "test", Columns: columns}

	e := canal.RowsEvent{Table: &table, Action: canal.UpdateAction, Rows: rows}
	handler.GetBinLogData(&model, &e, 1)

	if model.Int != updateRows[0] {
		t.Errorf("Int value did not update.")
	}
	if model.Bool != true {
		t.Errorf("Bool value did not update.")
	}
	if model.Float != updateRows[2] {
		t.Errorf("Float value did not update.")
	}
	if model.Enum != "Deleted" {
		t.Errorf("Enum value did not update.")
	}
	if model.String != updateRows[4] {
		t.Errorf("String value did not update.")
	}
	timeValue, _ := time.Parse("2006-01-02 15:04:05", updateRows[5].(string))
	if model.Time.Unix() != timeValue.Unix() {
		t.Errorf("DateTime value did not update.")
	}
	if model.String != updateRows[4].(string) {
		t.Errorf("String value did not update.")
	}
	if model.EnumNull != "" {
		t.Errorf("Enum nulled did not update.")
	}

}

func TestPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	model := binlogInvalidStruct{}
	handler := BinlogParser{}
	rows := make([][]interface{}, 1)
	insertRows := make([]interface{}, 6)
	insertRows[0] = 1

	rows[0] = insertRows

	columns := make([]schema.TableColumn, 6)

	columns[0] = schema.TableColumn{Name: "int", Type: schema.TYPE_NUMBER}

	table := schema.Table{Schema: "test", Name: "test", Columns: columns}

	e := canal.RowsEvent{Table: &table, Action: canal.InsertAction, Rows: rows}
	handler.GetBinLogData(&model, &e, 0)
}

func TestJson(t *testing.T) {
	model := JsonData{}
	handler := BinlogParser{}
	rows := make([][]interface{}, 1)
	insertRows := make([]interface{}, 6)
	insertRows[0] = 1
	insertRows[1] = `{"int":1,"test":"test"}`
	insertRows[2] = `{"a":"a","b":"b"}`
	insertRows[3] = `[2,4,6}`
	rows[0] = insertRows

	columns := make([]schema.TableColumn, 6)

	columns[0] = schema.TableColumn{Name: "int", Type: schema.TYPE_NUMBER}
	columns[1] = schema.TableColumn{Name: "struct_data", Type: schema.TYPE_STRING}
	columns[2] = schema.TableColumn{Name: "map_data", Type: schema.TYPE_STRING}
	columns[3] = schema.TableColumn{Name: "slice_data", Type: schema.TYPE_STRING}
	table := schema.Table{Schema: "test", Name: "test", Columns: columns}

	e := canal.RowsEvent{Table: &table, Action: canal.InsertAction, Rows: rows}
	handler.GetBinLogData(&model, &e, 0)
	if model.StructData.Test != "test" || model.StructData.Int != 1 {
		t.Errorf("Struct from json parsing failed.")
	}
	if len(model.SliceData) != 3 || model.SliceData[0] != 2 || model.SliceData[2] != 6 {
		t.Errorf("Sliced json parsing failed.")
	}

	if val, ok := model.MapData["a"]; ok && val != "a" && len(model.MapData) != 2 {
		t.Errorf("Map json parsing failed.")
	}
}

type binlogTestStruct struct {
	Int             int       `gorm:"column:int"`
	Bool            bool      `gorm:"column:bool"`
	Float           float64   `gorm:"column:float"`
	Enum            string    `gorm:"column:enum"`
	String          string    `gorm:"column:string"`
	Time            time.Time `gorm:"column:time"`
	EnumNull        string    `gorm:"column:enum_null"`
	ByteText        string    `gorm:"column:byte_text"`
	WillNotParse    int
	WillNotParseAlt int       `gorm:"column"`
}

type binlogInvalidStruct struct {
	Int int `gorm:"column:id"`
}

type JsonData struct {
	Int        int               `gorm:"column:int"`
	StructData TestData          `gorm:"column:struct_data;fromJson"`
	MapData    map[string]string `gorm:"column:map_data;fromJson"`
	SliceData  []int             `gorm:"column:slice_data;fromJson"`
}

type TestData struct {
	Test string `json:"test"`
	Int  int    `json:"int"`
}
