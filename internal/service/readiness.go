package service

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// ErrShuttingDown reports that graceful shutdown has started.
var ErrShuttingDown = errors.New("service: shutting down")

// Probe verifies one critical dependency for the readiness endpoint.
type Probe struct {
	name  string
	check func(context.Context) error
}

// NewProbe builds a named readiness probe.
func NewProbe(name string, check func(context.Context) error) Probe {
	return Probe{name: name, check: check}
}

// Name returns the probe name used in readiness failures.
func (p Probe) Name() string { return p.name }

// Check runs the probe.
func (p Probe) Check(ctx context.Context) error {
	if p.check == nil {
		return nil
	}
	return p.check(ctx)
}

// Readiness answers GET /readyz: it fails while shutting down and whenever a
// critical dependency (PostgreSQL, Kafka) is unreachable.
type Readiness struct {
	probes       []Probe
	timeout      time.Duration
	shuttingDown atomic.Bool
}

// NewReadiness builds the readiness gate around the critical-dependency probes.
func NewReadiness(probes []Probe, timeout time.Duration) *Readiness {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Readiness{probes: probes, timeout: timeout}
}

// BeginShutdown marks the instance unready so the load balancer drains it.
func (r *Readiness) BeginShutdown() { r.shuttingDown.Store(true) }

// ShuttingDown reports whether graceful shutdown has started.
func (r *Readiness) ShuttingDown() bool { return r.shuttingDown.Load() }

// Check reports nil when the instance may receive traffic.
func (r *Readiness) Check(ctx context.Context) error {
	if r.shuttingDown.Load() {
		return ErrShuttingDown
	}
	var failures []error
	for _, probe := range r.probes {
		probeCtx, cancel := context.WithTimeout(ctx, r.timeout)
		err := probe.Check(probeCtx)
		cancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", probe.Name(), err))
		}
	}
	return errors.Join(failures...)
}
