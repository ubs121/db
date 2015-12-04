package mongo

import (
	"net/url"
	"testing"
)

/*
func TestQuery(t *testing.T) {
	session := user.GetSession()
	defer session.Close()

	m := map[string]interface{}{"name": "Хөгжүүлэгч"}

	c := session.DB("erp").C("Job")
	n, err := c.Find(m).Count()
	if err == nil {
		println(n)
	}
}*/

func TestUrlDecode(t *testing.T) {
	u, err := url.Parse("Asp%2FAcnt")
	if err != nil {
		panic(err)
	}
	print(u)
}
