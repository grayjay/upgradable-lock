An upgradable read-write lock.  Threads can acquire the lock in read, write, or upgradable mode.  One thread can use upgradable mode at a time.  It can upgrade
to exclude other threads or downgrade to let other threads use read mode.  This allows more concurrent access than a read-write lock.
