package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// readMethods are the gRPC calls that only read workflow state; WithPublicReads
// and GRPCAuth's publicReads leave them open. Polls are not reads: they lease tasks.
var readMethods = map[string]bool{
	WorkflowService_ListWorkflows_FullMethodName:    true,
	WorkflowService_GetWorkflow_FullMethodName:      true,
	WorkflowService_ListWorkflowRuns_FullMethodName: true,
}

// validBearer reports whether header is exactly "Bearer <token>", compared in
// constant time.
func validBearer(header, token string) bool {
	got, ok := strings.CutPrefix(header, "Bearer ")
	return ok && subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}

// BearerAuth requires "Authorization: Bearer <token>" on every request to next.
// An empty token disables the check.
func BearerAuth(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validBearer(r.Header.Get("Authorization"), token) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="workflowd"`)
			http.Error(w, "missing or invalid bearer token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isPublicRead reports whether r only reads workflows (list or describe).
func isPublicRead(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	return r.URL.Path == "/api/workflows" || strings.HasPrefix(r.URL.Path, "/api/workflows/")
}

// GRPCAuth returns server options that require the bearer token in the
// "authorization" metadata of every call, as the JSON API does. With
// publicReads, ListWorkflows, GetWorkflow and ListWorkflowRuns stay open. An empty token
// returns no options, so the server is unauthenticated as before.
func GRPCAuth(token string, publicReads bool) []grpc.ServerOption {
	if token == "" {
		return nil
	}
	return GRPCAuthorize(NewAuthorizer(token, publicReads, nil))
}

// MetadataNamespace is the gRPC metadata key that names a call's namespace.
const MetadataNamespace = "workflowd-namespace"

// GRPCAuthorize returns server options that resolve every call's namespace
// and token into a Grant the way the JSON API does, refusing calls a grant
// does not allow. A server without them serves the default namespace only.
func GRPCAuthorize(authz *Authorizer) []grpc.ServerOption {
	check := func(ctx context.Context, method string) (context.Context, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		token := ""
		if v := md.Get("authorization"); len(v) > 0 {
			var ok bool
			if token, ok = strings.CutPrefix(v[0], "Bearer "); !ok {
				token = "\x00" // malformed: an invalid token, not a missing one
			}
		}
		requested := ""
		if v := md.Get(MetadataNamespace); len(v) > 0 {
			requested = v[0]
		}
		g, err := authz.Authorize(token, requested, readMethods[method])
		switch {
		case err == nil:
			return context.WithValue(ctx, grantKey{}, g), nil
		case errors.Is(err, ErrForbidden):
			return nil, status.Error(codes.PermissionDenied, err.Error())
		case errors.Is(err, ErrInvalidArgument):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		default:
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}
	}
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			ctx, err := check(ctx, info.FullMethod)
			if err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if _, err := check(ss.Context(), info.FullMethod); err != nil {
				return err
			}
			return handler(srv, ss)
		}),
	}
}

// grantFrom is a gRPC call's grant; a server without GRPCAuthorize serves
// the default namespace.
func grantFrom(ctx context.Context) Grant {
	if g, ok := ctx.Value(grantKey{}).(Grant); ok {
		return g
	}
	return Grant{Namespace: DefaultNamespace, Anonymous: true}
}

var errMissingToken = errors.New("missing or invalid bearer token")
