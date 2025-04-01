package sql

import "fmt"

type DatasourceName struct {
	Host     string
	Port     int
	User     string
	Password string
	Dbname   string
}

func NewDatasourceName(host string, port int, user string, password string, dbname string) *DatasourceName {
	return &DatasourceName{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Dbname:   dbname,
	}
}
func (i *DatasourceName) String() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		i.Host, i.Port, i.User, i.Password, i.Dbname,
	)
}
