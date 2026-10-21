// Package sdkctx holds context keys shared by the public SDK packages, so the
// test environment can give activities the same context a worker does.
package sdkctx

// ActivityInfoKey keys a worker.ActivityInfo in an activity's context.
type ActivityInfoKey struct{}
