package mycrud

import (
	"database/sql"
	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type obj struct {
	db *sql.DB
	list  []string
	assoc map[string]string
	table string
}

var (
	errNoTable = errors.New("no table name set")
	rowsEmpty = errors.New("rows_empty")
	noRowsAffected = errors.New("no_rows_affected")
)

type Handler interface {
	Select(...string) Handler
	Set(map[string]string) Handler
	Table(string) Handler
	Join(string) Handler
	One(string) (map[string]string, error)
	All(string) ([]map[string]string, error)
	Exists(string) (bool, error)
	Put() error
	Update(string) error
}

var _ Handler = (*obj) (nil)

func Dial(dsn string, pool int) (Handler, error) {
	db, e := sql.Open("mysql", dsn)
	if e != nil {
		return nil, e
	}
	e = db.Ping()
	if e != nil {
		return nil, e// proper error handling instead of panic in your app
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(pool)
	db.SetMaxIdleConns(pool)

	return &obj {db: db}, nil
}
