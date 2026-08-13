package s3fs

import (
	"context"
	"sync"
)

// Locker is a pluggable cross-process lock backend, configured with
// WithLocker. Lock blocks until the lock named name is acquired or ctx is
// done; Unlock releases it and is idempotent. name is the cleaned absolute
// path of the locked file.
//
// S3FS already serializes lock holders per name within the process, so an
// implementation only arbitrates between processes: Lock is never called
// concurrently for the same name by one S3FS instance, and reentrancy is
// not required. S3Locker implements Locker on top of S3 conditional
// writes; backends such as Redis or etcd can be plugged in the same way.
type Locker interface {
	Lock(ctx context.Context, name string) error
	Unlock(ctx context.Context, name string) error
}

// lockManager implements flock-like advisory locking for file handles:
// exclusive, blocking, owned per handle, reentrant per owner, released on
// Close. When ext is set, holding a lock additionally requires the
// cross-process lock of the same name.
type lockManager struct {
	ext Locker

	mu    sync.Mutex
	locks map[string]*pathLock
}

type pathLock struct {
	owner   any
	waiters int
	// freed is closed and replaced on each release to wake waiters.
	freed chan struct{}
}

func newLockManager() *lockManager {
	return &lockManager{locks: make(map[string]*pathLock)}
}

// lock blocks until owner holds the lock on path. A second lock by the
// same owner is a no-op, like flock.
func (m *lockManager) lock(ctx context.Context, path string, owner any) error {
	fresh, err := m.acquireLocal(ctx, path, owner)
	if err != nil || !fresh || m.ext == nil {
		return err
	}
	if err := m.ext.Lock(ctx, path); err != nil {
		m.releaseLocal(path, owner)
		return err
	}
	return nil
}

// unlock releases owner's hold on path. Unlocking a lock not held is a
// no-op returning nil, like flock. On external backend failure the lock
// stays held so the caller may retry.
func (m *lockManager) unlock(ctx context.Context, path string, owner any) error {
	if !m.holds(path, owner) {
		return nil
	}
	if m.ext != nil {
		if err := m.ext.Unlock(ctx, path); err != nil {
			return err
		}
	}
	m.releaseLocal(path, owner)
	return nil
}

// releaseOnClose drops owner's hold like unlock, but never fails: a closed
// handle must always lose its lock (flock parity), so external backend
// errors are ignored.
func (m *lockManager) releaseOnClose(ctx context.Context, path string, owner any) {
	if !m.holds(path, owner) {
		return
	}
	if m.ext != nil {
		_ = m.ext.Unlock(ctx, path)
	}
	m.releaseLocal(path, owner)
}

// acquireLocal blocks until owner holds the in-process lock. fresh is
// false when owner already held it.
func (m *lockManager) acquireLocal(ctx context.Context, path string, owner any) (fresh bool, err error) {
	m.mu.Lock()
	for {
		e := m.locks[path]
		if e == nil {
			e = &pathLock{freed: make(chan struct{})}
			m.locks[path] = e
		}
		if e.owner == nil {
			e.owner = owner
			m.mu.Unlock()
			return true, nil
		}
		if e.owner == owner {
			m.mu.Unlock()
			return false, nil
		}
		freed := e.freed
		e.waiters++
		m.mu.Unlock()
		select {
		case <-freed:
			m.mu.Lock()
			e.waiters--
		case <-ctx.Done():
			m.mu.Lock()
			e.waiters--
			m.cleanupLocked(path, e)
			m.mu.Unlock()
			return false, ctx.Err()
		}
	}
}

func (m *lockManager) holds(path string, owner any) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.locks[path]
	return e != nil && e.owner == owner
}

func (m *lockManager) releaseLocal(path string, owner any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.locks[path]
	if e == nil || e.owner != owner {
		return
	}
	e.owner = nil
	close(e.freed)
	e.freed = make(chan struct{})
	m.cleanupLocked(path, e)
}

// cleanupLocked drops the map entry once unowned and unawaited. Caller
// must hold m.mu.
func (m *lockManager) cleanupLocked(path string, e *pathLock) {
	if e.owner == nil && e.waiters == 0 {
		delete(m.locks, path)
	}
}
