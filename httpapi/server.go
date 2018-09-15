package httpapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/serverless/event-gateway-connector/connection"
)

// ConfigAPI creates a new configuration API server.
func ConfigAPI(store connection.Service, port int) *http.Server {
	router := httprouter.New()

	api := &HTTPAPI{Connections: store}
	api.RegisterRoutes(router)

	handler := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      router,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}

	return handler
}
