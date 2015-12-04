package bolt

import "net/http"

func ImportTriple(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	_load_n3(r.Body)

	rpc.WriteJson(r, w, "OK", nil)
}
