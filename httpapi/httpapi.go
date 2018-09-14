package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/serverless/event-gateway-connector/connection"
)

// HTTPAPI exposes REST API for configuring Connector
type HTTPAPI struct {
	Connections connection.Service
}

// RegisterRoutes register HTTP API routes
func (h HTTPAPI) RegisterRoutes(router *httprouter.Router) {
	router.POST("/v1/spaces/:space/connections", h.createConnection)
	router.DELETE("/v1/spaces/:space/connections/:id", h.deleteConnection)
}

func (h HTTPAPI) createConnection(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)

	conn := &connection.Connection{}
	err := json.NewDecoder(r.Body).Decode(conn)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		encoder.Encode(&Response{Errors: []Error{{Message: err.Error()}}})
		return
	}

	conn.Space = params.ByName("space")
	output, err := h.Connections.CreateConnection(conn)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		encoder.Encode(&Response{Errors: []Error{{Message: err.Error()}}})
		return
	}

	w.WriteHeader(http.StatusCreated)
	encoder.Encode(output)
}

func (h HTTPAPI) deleteConnection(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)

	space := params.ByName("space")
	if err := h.Connections.DeleteConnection(space, connection.ID(params.ByName("id"))); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		encoder.Encode(&Response{Errors: []Error{{Message: err.Error()}}})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
