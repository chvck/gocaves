package mock

import "github.com/couchbaselabs/gocaves/mock/mockauth"

// QueryService represents a query service running somewhere in the cluster.
type QueryService interface {
	// Node returns the node which owns this service.
	Node() ClusterNode

	// Hostname returns the hostname where this service can be accessed.
	Hostname() string

	// ListenPort returns the port this service is listening on.
	ListenPort() int

	// ListenPortTLS returns the TLS port this service is listening on.
	ListenPortTLS() int

	// Close will shut down this service once it is no longer needed.
	Close() error

	// CheckAuthenticated verifies that the currently authenticated user has the specified permissions.
	CheckAuthenticated(permission mockauth.Permission, bucket, scope, collection string, request *HTTPRequest) bool
}
