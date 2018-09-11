package httpapi

import (
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/serverless/event-gateway-connector/connection"
)

// StartConfigAPI creates a new configuration API server and listens for requests.
func StartConfigAPI(store connection.Service) error {
	router := httprouter.New()

	api := &HTTPAPI{Connections: store}
	api.RegisterRoutes(router)

	handler := &http.Server{
		Addr:         ":4002",
		Handler:      router,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}

	return handler.ListenAndServe()
}
