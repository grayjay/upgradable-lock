upgradable-lock
===============

An upgradable read-write lock. Threads can acquire the lock in read, write, or upgradable mode. A thread using upgradable mode can upgrade to exclude other threads or downgrade to let other threads use read mode. This can allow more concurrent access when the lock is held for significant lengths of time.

Documentation: <http://wordroute.com/grayjay/upgradable-lock-2.0.0>
