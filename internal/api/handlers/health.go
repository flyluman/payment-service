package handlers

import (
	"context"
	"net/http"
	"time"
)

const healthCheckTimeout = 2 * time.Second

type Pinger interface {
	Ping(ctx context.Context) error
}

type Check struct {
	Name   string
	Pinger Pinger
}

type HealthHandler struct{ checks []Check }

func NewHealthHandler(checks ...Check) *HealthHandler { return &HealthHandler{checks: checks} }

type healthResponse struct {
	Status     string            `json:"status"`
	Components map[string]string `json:"components"`
}

func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	components := map[string]string{}
	healthy := true

	for _, c := range h.checks {
		ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
		err := c.Pinger.Ping(ctx)
		cancel()
		if err != nil {
			components[c.Name] = "unhealthy"
			healthy = false
		} else {
			components[c.Name] = "healthy"
		}
	}

	status := http.StatusOK
	overall := "healthy"
	if !healthy {
		status = http.StatusServiceUnavailable
		overall = "unhealthy"
	}

	writeJSON(w, r, status, healthResponse{Status: overall, Components: components})
}
