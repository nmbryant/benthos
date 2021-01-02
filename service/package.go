// Package service provides a high level API for creating a Benthos service,
// registering custom plugin components, customizing the availability of native
// components, and running it.
package service

// Manager provides access to service-wide observability components as well as
// resources.
type Manager interface {
	// TODO
}

// ConfigConstructor returns a struct containing configuration fields for a
// plugin implementation with default values. This struct will be
// marshalled/unmarshalled as YAML using gopkg.in/yaml.v3.
type ConfigConstructor func() interface{}
