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
	WorkflowService_ListWorkflows_FullMethodName: true,
	WorkflowService_GetWorkflow_FullMethodName:   true,
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
// publicReads, ListWorkflows and GetWorkflow stay open. An empty token
// returns no options, so the server is unauthenticated as before.
func GRPCAuth(token string, publicReads bool) []grpc.ServerOption {
	if token == "" {
		return nil
	}
	check := func(ctx context.Context, method string) error {
		if publicReads && readMethods[method] {
			return nil
		}
		md, _ := metadata.FromIncomingContext(ctx)
		for _, v := range md.Get("authorization") {
			if validBearer(v, token) {
				return nil
			}
		}
		return status.Error(codes.Unauthenticated, errMissingToken.Error())
	}
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := check(ctx, info.FullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := check(ss.Context(), info.FullMethod); err != nil {
				return err
			}
			return handler(srv, ss)
		}),
	}
}

var errMissingToken = errors.New("missing or invalid bearer token")
