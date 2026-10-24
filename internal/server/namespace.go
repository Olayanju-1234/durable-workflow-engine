package server

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

// Namespaces (ADR-0015) isolate tenants inside one engine. Each has its own
// workflows, schedules, workflow types, task queues and retention, and its
// own tokens; a namespace's token reaches that namespace and nothing else.
// Inside the engine a workflow is named by its store key (history.Key):
// the bare id in the default namespace, "<namespace>/<id>" elsewhere.
const DefaultNamespace = history.DefaultNamespace

// Errors the API layers map to 401, 403 and 400.
var (
	ErrUnauthenticated = errors.New("server: missing or invalid bearer token")
	ErrForbidden       = errors.New("server: forbidden")
)

// nsOf is the namespace a store key belongs to.
func nsOf(key string) string {
	ns, _ := history.SplitKey(key)
	return ns
}

// localID is the workflow id a store key names, without its namespace.
func localID(key string) string {
	_, id := history.SplitKey(key)
	return id
}

// nsName is a name scoped to a namespace: a workflow type, an activity's step
// name or a schedule id.
type nsName struct{ ns, name string }

// checkNamespace refuses a name that cannot name a namespace.
func checkNamespace(ns string) error {
	if !history.ValidNamespace(ns) {
		return fmt.Errorf("%w: invalid namespace %q: use 1 to 63 lowercase letters, digits and hyphens", ErrInvalidArgument, ns)
	}
	return nil
}

// NamespaceConfig configures one namespace. The default namespace always
// exists; configuring it only changes its settings.
type NamespaceConfig struct {
	Name string
	// Tokens grant this namespace (plain bearer tokens), as do tokens whose
	// SHA-256 is in TokenSHA256 (hex). A namespace other than default with
	// neither is reachable only with the engine's own token.
	Tokens      []string
	TokenSHA256 []string
	// Retention overrides the engine's --retention for this namespace's
	// finished workflows; nil inherits it, and zero keeps them forever.
	Retention *time.Duration
}

// Namespace is a configured namespace.
type Namespace struct {
	Name      string
	Retention *time.Duration
	tokens    [][sha256.Size]byte
}

// NamespaceSet is the engine's configured namespaces. It always holds default.
type NamespaceSet struct {
	byName map[string]*Namespace
}

// NewNamespaceSet validates cfgs: names are unique and valid, and a token
// grants exactly one namespace.
func NewNamespaceSet(cfgs []NamespaceConfig) (*NamespaceSet, error) {
	set := &NamespaceSet{byName: map[string]*Namespace{DefaultNamespace: {Name: DefaultNamespace}}}
	owner := map[[sha256.Size]byte]string{}
	seen := map[string]bool{}
	for _, c := range cfgs {
		if err := checkNamespace(c.Name); err != nil {
			return nil, err
		}
		if seen[c.Name] {
			return nil, fmt.Errorf("namespace %q is configured twice", c.Name)
		}
		seen[c.Name] = true
		if c.Retention != nil && *c.Retention < 0 {
			return nil, fmt.Errorf("namespace %q: retention must not be negative", c.Name)
		}
		ns := &Namespace{Name: c.Name, Retention: c.Retention}
		for _, t := range c.Tokens {
			if strings.TrimSpace(t) == "" {
				return nil, fmt.Errorf("namespace %q: empty token", c.Name)
			}
			ns.tokens = append(ns.tokens, sha256.Sum256([]byte(t)))
		}
		for _, h := range c.TokenSHA256 {
			b, err := hex.DecodeString(strings.TrimSpace(h))
			if err != nil || len(b) != sha256.Size {
				return nil, fmt.Errorf("namespace %q: token_sha256 %q is not a hex SHA-256", c.Name, h)
			}
			ns.tokens = append(ns.tokens, [sha256.Size]byte(b))
		}
		for _, h := range ns.tokens {
			if other, dup := owner[h]; dup {
				return nil, fmt.Errorf("namespaces %q and %q share a token; a token must grant one namespace", other, c.Name)
			}
			owner[h] = c.Name
		}
		set.byName[c.Name] = ns
	}
	return set, nil
}

// namespaceFile is the JSON form of --namespaces.
type namespaceFile struct {
	Namespaces []struct {
		Name        string   `json:"name"`
		TokenFile   string   `json:"token_file"`
		TokenSHA256 []string `json:"token_sha256"`
		Retention   *string  `json:"retention"`
	} `json:"namespaces"`
}

// LoadNamespaces reads a --namespaces file. A relative token_file is read
// from the file's own directory.
func LoadNamespaces(path string) (*NamespaceSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("namespaces: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var f namespaceFile
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("namespaces: %s: %w", path, err)
	}
	cfgs := make([]NamespaceConfig, 0, len(f.Namespaces))
	for _, n := range f.Namespaces {
		c := NamespaceConfig{Name: n.Name, TokenSHA256: n.TokenSHA256}
		if n.TokenFile != "" {
			tf := n.TokenFile
			if !filepath.IsAbs(tf) {
				tf = filepath.Join(filepath.Dir(path), tf)
			}
			b, err := os.ReadFile(tf)
			if err != nil {
				return nil, fmt.Errorf("namespaces: %s: token_file: %w", n.Name, err)
			}
			c.Tokens = append(c.Tokens, strings.TrimSpace(string(b)))
		}
		if n.Retention != nil {
			d, err := time.ParseDuration(*n.Retention)
			if err != nil {
				return nil, fmt.Errorf("namespaces: %s: retention: %w", n.Name, err)
			}
			c.Retention = &d
		}
		cfgs = append(cfgs, c)
	}
	set, err := NewNamespaceSet(cfgs)
	if err != nil {
		return nil, fmt.Errorf("namespaces: %s: %w", path, err)
	}
	return set, nil
}

// Get returns a configured namespace, or nil.
func (s *NamespaceSet) Get(name string) *Namespace {
	if s == nil {
		if name == DefaultNamespace {
			return &Namespace{Name: DefaultNamespace}
		}
		return nil
	}
	return s.byName[name]
}

// Names lists the configured namespaces, default first.
func (s *NamespaceSet) Names() []string {
	if s == nil {
		return []string{DefaultNamespace}
	}
	names := make([]string, 0, len(s.byName))
	for n := range s.byName {
		if n != DefaultNamespace {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return append([]string{DefaultNamespace}, names...)
}

// byToken is the namespace a token grants, or nil. Every configured hash is
// compared, in constant time, so timing says nothing about which matched.
func (s *NamespaceSet) byToken(token string) *Namespace {
	if s == nil {
		return nil
	}
	h := sha256.Sum256([]byte(token))
	var found *Namespace
	for _, ns := range s.byName {
		for _, t := range ns.tokens {
			if subtle.ConstantTimeCompare(h[:], t[:]) == 1 {
				found = ns
			}
		}
	}
	return found
}

// Grant is what a caller may do: one namespace, and whether it holds the
// engine's own token (every namespace) or no token at all.
type Grant struct {
	Namespace string
	Admin     bool
	Anonymous bool
}

// Authorizer turns a call's bearer token and requested namespace into a
// Grant. Both transports use it, so HTTP and gRPC agree on every rule:
//
//   - the engine token (--auth-token) reaches every namespace;
//   - a namespace's token reaches that namespace, which is also where a call
//     naming none goes, and is refused (403) anywhere else;
//   - without a token, only the default namespace, and only when it is open:
//     no engine token and no default-namespace token are set (or, with public
//     reads, for reads).
type Authorizer struct {
	adminToken  string
	publicReads bool
	namespaces  *NamespaceSet
}

// NewAuthorizer builds an Authorizer. An empty adminToken and a nil set give
// the open single-namespace engine every release before 0.4.0 was.
func NewAuthorizer(adminToken string, publicReads bool, namespaces *NamespaceSet) *Authorizer {
	return &Authorizer{adminToken: adminToken, publicReads: publicReads, namespaces: namespaces}
}

// Namespaces is the configured set (nil: only default).
func (a *Authorizer) Namespaces() *NamespaceSet {
	if a == nil {
		return nil
	}
	return a.namespaces
}

// Open reports whether the default namespace takes calls without a token.
func (a *Authorizer) Open() bool { return a.open() }

// open reports whether the default namespace takes calls without a token.
func (a *Authorizer) open() bool {
	if a == nil {
		return true
	}
	d := a.namespaces.Get(DefaultNamespace)
	return a.adminToken == "" && (d == nil || len(d.tokens) == 0)
}

// Authorize decides which namespace a call may use. token is the bearer token
// presented ("" for none), requested the namespace the call names ("" for
// none), and read whether the call only reads workflow state.
func (a *Authorizer) Authorize(token, requested string, read bool) (Grant, error) {
	if requested != "" {
		if err := checkNamespace(requested); err != nil {
			return Grant{}, err
		}
	}
	if a == nil {
		if requested != "" && requested != DefaultNamespace {
			return Grant{}, fmt.Errorf("%w: namespace %q needs a token", ErrUnauthenticated, requested)
		}
		return Grant{Namespace: DefaultNamespace, Anonymous: token == ""}, nil
	}
	if token != "" {
		if a.adminToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(a.adminToken)) == 1 {
			if requested == "" {
				requested = DefaultNamespace
			}
			return Grant{Namespace: requested, Admin: true}, nil
		}
		ns := a.namespaces.byToken(token)
		if ns == nil {
			return Grant{}, ErrUnauthenticated
		}
		if requested != "" && requested != ns.Name {
			return Grant{}, fmt.Errorf("%w: this token is for namespace %q, not %q", ErrForbidden, ns.Name, requested)
		}
		return Grant{Namespace: ns.Name}, nil
	}
	if requested == "" {
		requested = DefaultNamespace
	}
	if requested != DefaultNamespace {
		return Grant{}, fmt.Errorf("%w: namespace %q needs a token", ErrUnauthenticated, requested)
	}
	if a.open() || (read && a.publicReads) {
		return Grant{Namespace: DefaultNamespace, Anonymous: true}, nil
	}
	return Grant{}, ErrUnauthenticated
}

// Visible lists the namespaces a grant can see: all of them for the engine
// token, else its own.
func (a *Authorizer) Visible(g Grant) []string {
	if g.Admin {
		return a.Namespaces().Names()
	}
	return []string{g.Namespace}
}
