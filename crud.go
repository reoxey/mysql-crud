package mycrud

import (
	"database/sql"
	"strings"
)

func (o *obj) Select(f ...string) Handler {
	o.list = f
	return o
}

func (o *obj) Set(m map[string]string) Handler {
	o.assoc = m
	return o
}

func (o *obj) Table(t string) Handler {
	o.table = t
	return o
}

func (o *obj) Join(t string) Handler {
	o.table += " JOIN " + t
	return o
}

func (o *obj) All(w string) ([]map[string]string, error) {
	if w == "" {
		w = "1"
	}
	if o.table == "" {
		return nil, errNoTable
	}

	r, e := o.db.Query("SELECT "+strings.Join(o.list, ",")+" FROM "+o.table+" WHERE "+w)
	if e != nil {
		return nil, e
	}

	columns, e := r.Columns()
	if e != nil {
		return nil, e
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var s []map[string]string
	for r.Next() {
		e = r.Scan(scanArgs...)
		if e != nil {
			return nil, e
		}

		m := make(map[string]string)
		var value string
		for i, col := range values {
			if col == nil {
				value = ""
			} else {
				value = string(col)
			}
			m[columns[i]] = value
		}
		s = append(s, m)
	}
	if e = r.Err(); e != nil {
		return nil, e
	}

	return s, nil
}

func (o *obj) One(w string) (map[string]string, error) {
	if w == "" {
		w = "1"
	}
	if o.table == "" {
		return nil, errNoTable
	}
	if len(o.list) == 0 {
		o.list = []string{"*"}
	}
	r, e := o.db.Query("SELECT "+strings.Join(o.list, ",")+" FROM "+o.table+" WHERE "+w + " Limit 1")
	if e != nil {
		return nil, e
	}

	columns, e := r.Columns()
	if e != nil {
		return nil, e
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	ok := false
	m := make(map[string]string)
	for r.Next() {
		e = r.Scan(scanArgs...)
		if e != nil {
			return nil, e
		}

		var value string
		for i, col := range values {
			if col == nil {
				value = ""
			} else {
				value = string(col)
			}
			m[columns[i]] = value
		}
		ok = true
	}
	if e = r.Err(); e != nil {
		return nil, e
	}

	if ok {
		return m, nil
	}

	return nil, rowsEmpty
}

func (o *obj) Exists(w string) (bool, error) {
	if w == "" {
		w = "1"
	}

	if o.table == "" {
		return false, errNoTable
	}

	if len(o.list) == 0 {
		o.list = []string{"*"}
	}
	r, e := o.db.Query("SELECT "+strings.Join(o.list, ",")+" FROM "+o.table+" WHERE "+w + " Limit 1")
	if e != nil {
		return false, e
	}

	ok := false
	for r.Next() {
		ok = true
	}

	return ok, nil
}

func (o *obj) Put() error {

	if o.table == "" {
		return errNoTable
	}

	var val []string
	for k, v := range o.assoc {
		val = append(val, k+"='"+v+"'")
	}

	r, e := o.db.Exec("INSERT "+o.table+" SET "+strings.Join(val, ","))

	if e != nil {
		return e
	}

	if n, _ := r.RowsAffected(); n == 0 {
		return noRowsAffected
	}

	return e
}

func (o *obj) Update(w string) error {

	if o.table == "" {
		return errNoTable
	}

	var val []string
	for k, v := range o.assoc {
		val = append(val, k+"='"+v+"'")
	}

	if w == "" {
		w = "1"
	}

	r, e := o.db.Exec("UPDATE "+o.table+" SET "+strings.Join(val, ",")+" WHERE "+w)

	if e != nil {
		return e
	}

	if n, _ := r.RowsAffected(); n == 0 {
		return noRowsAffected
	}

	return nil
}