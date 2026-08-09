package httpx

import (
	"crypto/tls"
	"net/http/httptest"
	"testing"
)

func TestIsHTTPS(t *testing.T) {
	cases := []struct {
		name  string
		tls   bool
		proto string
		want  bool
	}{
		{"plain http", false, "", false},
		{"direct tls", true, "", true},
		{"proxy header https", false, "https", true},
		{"proxy header http", false, "http", false},
		{"proxy header list takes first", false, "https, http", true},
		{"proxy header garbage", false, "httpsish", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/", nil)
		if c.tls {
			r.TLS = &tls.ConnectionState{}
		}
		if c.proto != "" {
			r.Header.Set("X-Forwarded-Proto", c.proto)
		}
		if got := IsHTTPS(r); got != c.want {
			t.Errorf("%s: IsHTTPS = %v, want %v", c.name, got, c.want)
		}
	}
}
