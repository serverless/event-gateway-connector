package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/serverless/event-gateway-connector/connection"
)

// HTTPAPI exposes REST API for configuring Connector
type HTTPAPI struct {
	Connections connection.Service
}

// RegisterRoutes register HTTP API routes
func (h HTTPAPI) RegisterRoutes(router *httprouter.Router) {
	router.Handler("GET", "/v1/metrics", promhttp.Handler())
	router.GET("/v1/spaces/:space/connections", h.listConnections)
	router.POST("/v1/spaces/:space/connections", h.createConnection)
	router.PUT("/v1/spaces/:space/connections/:id", h.updateConnection)
	router.DELETE("/v1/spaces/:space/connections/:id", h.deleteConnection)
}

func (h HTTPAPI) listConnections(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)

	space := params.ByName("space")
	connections, err := h.Connections.ListConnections(space)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		encoder.Encode(&Response{Errors: []Error{{Message: err.Error()}}})
	} else {
		encoder.Encode(&ConnectionsResponse{Connections: connections})
	}
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

func (h HTTPAPI) updateConnection(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
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
	conn.ID = connection.ID(params.ByName("id"))
	output, err := h.Connections.UpdateConnection(conn)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		encoder.Encode(&Response{Errors: []Error{{Message: err.Error()}}})
		return
	}

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

// ConnectionsResponse is a HTTPAPI JSON response containing connections.
type ConnectionsResponse struct {
	Connections []*connection.Connection `json:"connections"`
}
