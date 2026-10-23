// ESM entry point. The SDK is one CommonJS module (workflowd.cjs) so that
// `import` and `require` load the same copy: an ActivityFailure thrown by code
// that required it is still `instanceof ActivityFailure` where it was imported.
import sdk from './workflowd.cjs';

export const {
  WorkflowFailure,
  ActivityFailure,
  NonRetryableError,
  CancelledFailure,
  DEFAULT_VERSION,
  readPayload,
  decide,
  Worker,
} = sdk;
