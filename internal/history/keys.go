package history

import (
	"fmt"
	"strings"
)

// Namespaces (ADR-0015). A store key names one history. In the default
// namespace the key is the bare workflow id, so every directory written before
// namespaces existed reads unchanged. Elsewhere it is "<namespace>/<id>", and
// the file lives in that namespace's directory:
//
//	<data-dir>/<id>.history                       default
//	<data-dir>/namespaces/<namespace>/<id>.history every other namespace
//
// Workflow ids cannot contain "/", so a key has exactly one reading.
const (
	DefaultNamespace = "default"
	namespacesDir    = "namespaces"
)

// ValidNamespace reports whether name can name a namespace: 1 to 63 lowercase
// letters, digits and hyphens, starting and ending with a letter or digit. A
// DNS label is a safe directory name on every file system, case-insensitive
// ones included.
func ValidNamespace(name string) bool {
	if len(name) == 0 || len(name) > 63 || name[0] == '-' || name[len(name)-1] == '-' {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// Key is the store key of workflow id in namespace ns ("" is the default).
func Key(ns, id string) string {
	if ns == "" || ns == DefaultNamespace {
		return id
	}
	return ns + "/" + id
}

// SplitKey is Key's inverse: the namespace and workflow id a key names.
func SplitKey(key string) (ns, id string) {
	if i := strings.IndexByte(key, '/'); i >= 0 {
		return key[:i], key[i+1:]
	}
	return DefaultNamespace, key
}

// Namespaced reports whether key names a history outside the default namespace.
func Namespaced(key string) bool { return strings.IndexByte(key, '/') >= 0 }

// validateWorkflowID rejects keys that are empty, ambiguous or could escape
// the data directory.
func validateWorkflowID(key string) error {
	ns, id := SplitKey(key)
	if Namespaced(key) && (ns == DefaultNamespace || !ValidNamespace(ns)) {
		return fmt.Errorf("history: invalid namespace in key %q", key)
	}
	if id == "" {
		return fmt.Errorf("history: empty workflow ID in key %q", key)
	}
	if id == "." || id == ".." || strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("history: invalid workflow ID %q", id)
	}
	return nil
}
