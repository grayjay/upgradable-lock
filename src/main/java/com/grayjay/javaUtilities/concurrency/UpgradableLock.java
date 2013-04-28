package concurrency;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

/**
 * A reentrant read-write lock allowing at most one designated upgradable thread
 * that can switch between reading and writing. Other readers can acquire the
 * lock while the thread with the upgradable lock is downgraded. The upgradable
 * thread blocks while upgrading if other threads hold read locks.
 * <p> 
 * A thread can initially acquire the lock in any of three modes: read,
 * upgradable, and write. A thread acquiring an upgradable lock starts in the
 * downgraded state. All locks and unlocks are nested. This means that a thread
 * cannot acquire a write lock, then a read lock, and then release the write
 * lock without releasing the read lock. Calls to downgrade must be matched by
 * calls to upgrade. Calls to upgrade and downgrade can be interleaved with
 * calls to lock and unlock in any order, as long as the thread has an
 * upgradable or write lock when upgrading or downgrading. A thread with a read
 * lock cannot acquire an upgradable or write lock. Any thread with an
 * upgradable or write lock can acquire the lock again in any of the three
 * modes. The lock excludes readers if a thread has a write lock or has
 * upgraded. Acquiring a read lock after an upgradable or write lock has no
 * effect, though it still must be released.
 * <p>
 * This class allows condition waits for threads that hold write locks or have
 * upgraded.
 * <p>
 * This lock allows running threads to acquire the lock without waiting in the
 * queue, unless the current thread is acquiring a read lock, and a thread is
 * waiting to upgrade or acquire a write lock at the front of the queue.
 */
public final class UpgradableLock {
  /*
   * This class stores each thread's lock holds in a thread local variable. It
   * uses a subclass of AbstractQueuedSynchronizer (mySync) to manage the queue
   * and store the number of threads with each type of lock hold. For every call
   * to a public method, this class first uses the thread local state to
   * determine whether the state of mySync must change. Then it delegates to
   * mySync and updates the thread local state on success.
   */
  
  private static final long MIN_TIMEOUT = -1L;
  private static final long NO_WAIT = -2L;
  private static final long NO_TIMEOUT = -3L;
  
  private final Sync mySync = new Sync();
  private final ThreadLocal<ThreadState> myThreadState = new ThreadLocal<ThreadState>();
  
  /**
   * The modes used to acquire the lock.
   */
  public static enum Mode {
    READ,
    UPGRADABLE,
    WRITE
  }

  /**
   * Thrown when a thread attempts to acquire or upgrade the lock when the lock
   * already has the maximum number of holds.
   */
  public static class TooManyHoldsException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    TooManyHoldsException(String aMessage) {
      super(aMessage);
    }
  }
  
  /**
   * Lock state that applies to the current thread. It stores the numbers
   * and types of holds that the thread currently has.
   */
  private static class ThreadState {
    private static final int NO_WRITE_LOCK = -1;
    
    private final boolean myAcquiredReadFirst;
    private int myUpgradeCount;
    private int myLockCount;
    private int myFirstWriteLock = NO_WRITE_LOCK;

    ThreadState(boolean aIsRead) {
      myAcquiredReadFirst = aIsRead;
    }
    
    boolean acquiredReadFirst() {
      return myAcquiredReadFirst;
    }
    
    /**
     * Returns {@code true} if the thread holds only a read lock or a downgraded
     * upgradable lock.
     */
    boolean isDowngraded() {
      return myAcquiredReadFirst || myUpgradeCount == 0 && myFirstWriteLock == -1;
    }

    void incrementWrite() {
      incrementHolds();
      if (myFirstWriteLock == NO_WRITE_LOCK) {
        myFirstWriteLock = myLockCount;
      }
    }

    void incrementUpgradable() {
      incrementHolds();
    }

    void incrementRead() {
      incrementHolds();
    }

    /**
     * Removes the latest hold and returns {@code true} if there are no more
     * holds for the thread.
     */
    boolean decrementHolds() {
      if (myFirstWriteLock == myLockCount) myFirstWriteLock = NO_WRITE_LOCK;
      return --myLockCount == 0;
    }

    void upgrade() {
      if (myLockCount == Integer.MAX_VALUE) {
        throw new TooManyHoldsException("Too many upgrades");
      }
      myUpgradeCount++;
    }

    void downgrade() {
      myUpgradeCount--;
    }
    
    private void incrementHolds() {
      if (myLockCount == Integer.MAX_VALUE) {
        throw new TooManyHoldsException("Too many holds");
      }
      myLockCount++;
    }
  }

  private static class Sync extends AbstractQueuedSynchronizer {
    /*
     * This class uses its int state to maintain a count of threads with
     * read/downgraded locks and a count of threads with write/upgraded
     * locks. It does not keep track of the number of holds per thread or which
     * holds allow upgrading or downgrading. The lowest bit stores the number of
     * writer threads, and the rest of the state stores the number of reader
     * threads.
     */
    
    private static final int MAX_READ_HOLDS = Integer.MAX_VALUE >>> 1;
    
    /* Arguments passed to methods of AbstractQueuedSynchronizer
     * 
     * The values for acquiring and releasing a write lock must equal the lock
     * state before releasing a write lock for conditions to work.
     */
    private static final int LOCK_WRITE = calcState(1, 0);
    private static final int UNLOCK_WRITE = calcState(1, 0);
    private static final int UPGRADE = 0;
    private static final int DOWNGRADE = 0;
    private static final int UNUSED = 0;
    
    boolean acquire(boolean aIsUpgrade, boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
      int mArg = aIsUpgrade ? UPGRADE : LOCK_WRITE;
      if (aTime == NO_WAIT) {
        return tryAcquire(mArg);
      } else if (aTime == NO_TIMEOUT) {
        if (aInterruptible) {
          acquireInterruptibly(mArg); 
        } else acquire(mArg);
        return true;
      } else return tryAcquireNanos(mArg, aUnit.toNanos(aTime));
    }
    
    boolean acquireShared(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
      if (aTime == NO_WAIT) {
        return tryAcquireShared(UNUSED) >= 0;
      } else if (aTime == NO_TIMEOUT) {
        if (aInterruptible) {
          acquireSharedInterruptibly(UNUSED); 
        } else acquireShared(UNUSED);
        return true;
      } else return tryAcquireSharedNanos(UNUSED, aUnit.toNanos(aTime));
    }
    
    void releaseRead() {
      releaseShared(UNUSED);
    }
    
    void releaseWrite() {
      release(UNLOCK_WRITE);
    }
    
    void downgrade() {
      release(DOWNGRADE);
    }

    @Override
    protected boolean tryAcquire(int aArg) {
      boolean mIsUpgrade = aArg == UPGRADE;
      int mState = getState();
      if (hasWriteHold(mState)) return false;
      int mReadHolds = getReadHolds(mState);
      int mNewReadHolds = mIsUpgrade ? mReadHolds  - 1 : mReadHolds;
      if (mNewReadHolds == 0) {
        int mNewState = calcState(1, 0);
        if (compareAndSetState(mState, mNewState)) {
          setExclusiveOwnerThread(Thread.currentThread());
          return true;
        }
      }
      return false;
    }

    @Override
    protected boolean tryRelease(int aArg) {
      if (!isHeldExclusively()) return false;
      setExclusiveOwnerThread(null);
      boolean mIsDowngrade = aArg == DOWNGRADE;
      int mNewReadHolds = mIsDowngrade ? 1 : 0;
      int mNewState = calcState(0, mNewReadHolds);
      setState(mNewState);
      return true;
    }
    
    @Override
    protected int tryAcquireShared(int aUnused) {
      // prevent writer threads from starving
      if (isFirstQueuedThreadExclusive()) return -1;
      int mState = getState();
      if (hasWriteHold(mState)) return -1;
      int mReadHolds = getReadHolds(mState);
      if (mReadHolds == MAX_READ_HOLDS) throw new TooManyHoldsException("Too many holds");
      int mNewState = calcState(0, mReadHolds + 1);
      return compareAndSetState(mState, mNewState) ? 1 : -1;
    }

    @Override
    protected boolean tryReleaseShared(int aUnused) {
      int mState, mNewState;
      do {
        mState = getState();
        int mReadHolds = getReadHolds(mState);
        mNewState = calcState(0, mReadHolds - 1);
      } while (!compareAndSetState(mState, mNewState));
      return true;
    }
    
    @Override
    protected boolean isHeldExclusively() {
      return Thread.currentThread().equals(getExclusiveOwnerThread());
    }
    
    private static boolean hasWriteHold(int aState) {
      return (aState & 1) == 1;
    }
    
    private static int getReadHolds(int aState) {
      return aState >>> 1;
    }

    private static int calcState(int aWriteHolds, int aReadHolds) {
      return (aReadHolds << 1) | aWriteHolds;
    }
    
    private boolean isFirstQueuedThreadExclusive() {
      Thread mFirst = getFirstQueuedThread();
      return getExclusiveQueuedThreads().contains(mFirst);
    }
  }
  
  /**
   * Acquires the lock in the given locking mode.
   * 
   * @throws IllegalMonitorStateException
   *         if the thread already holds a read lock and is trying for an
   *         upgradable or write lock.
   */
  public void lock(Mode aMode) {
    try {
      lockInternal(aMode, false, NO_TIMEOUT, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new AssertionError();
    }
  }
  
  /**
   * Acquires the lock while allowing
   * {@linkplain Thread#interrupt interruption}.
   * 
   * @see UpgradableLock#lock(Mode)
   * @throws InterruptedException
   *         if the thread is interrupted before or while waiting for the lock.
   */
  public void lockInterruptibly(Mode aMode) throws InterruptedException {
    lockInternal(aMode, true, NO_TIMEOUT, TimeUnit.SECONDS);
  }
  
  /**
   * Acquires the lock only if it is currently
   * available and returns {@code true} if it succeeds.
   * 
   * @see UpgradableLock#lock(Mode)
   */
  public boolean tryLock(Mode aMode) {
    try {
      return lockInternal(aMode, false, NO_WAIT, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new AssertionError();
    }
  }
  
  /**
   * Tries to acquire the lock within the given time limit and returns
   * {@code true} if it succeeds.
   * 
   * @see UpgradableLock#lock(Mode)
   * @throws InterruptedException
   *         if the thread is interrupted before or while waiting for the lock.
   */
  public boolean tryLock(Mode aMode, long aTime, TimeUnit aUnit) throws InterruptedException {
    if (aTime < 0) aTime = MIN_TIMEOUT;
    return lockInternal(aMode, true, aTime, aUnit);
  }
  
  /**
   * Releases the thread's latest hold on the lock.
   * 
   * @throws IllegalMonitorStateException if the thread does not hold the lock.
   */
  public void unlock() {
    ThreadState mState = myThreadState.get();
    if (mState == null) {
      throw new IllegalMonitorStateException("Cannot unlock lock that was not held");
    }
    boolean mWasDowngraded = mState.isDowngraded();
    if (mState.decrementHolds()) {
      if (mWasDowngraded) mySync.releaseRead();
      else mySync.releaseWrite();
      myThreadState.remove();
    } else if (!mWasDowngraded && mState.isDowngraded()) {
      mySync.downgrade();
    }
  }
  
  /**
   * Upgrades the thread's hold on the lock. This has no effect if the thread
   * holds a write lock or has already upgraded.
   * 
   * @throws IllegalMonitorStateException
   *         if the thread does not already hold the lock in upgradable or write
   *         mode.
   */
  public void upgrade() {
    try {
      upgradeInternal(false, NO_TIMEOUT, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new AssertionError();
    }
  }

  /**
   * Upgrades the thread's hold on the lock while allowing
   * {@linkplain Thread#interrupt interruption}.
   * @see UpgradableLock#upgrade()
   * @throws InterruptedException
   *         if the thread is interrupted before or while waiting to upgrade.
   */
  public void upgradeInterruptibly() throws InterruptedException {
    upgradeInternal(true, NO_TIMEOUT, TimeUnit.SECONDS);
  }

  /**
   * Upgrades the thread's hold on the lock only if there are no current readers
   * and returns {@code true} if it succeeds.
   * @see UpgradableLock#upgrade()
   */
  public boolean tryUpgrade() {
    try {
      return upgradeInternal(false, NO_WAIT, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new AssertionError();
    }
  }

  /**
   * Tries to upgrade the thread's hold on the lock for the given amount of time
   * and returns {@code true} if it succeeds.
   * 
   * @see UpgradableLock#upgrade()
   * @throws InterruptedException
   *         if the thread is interrupted before or while waiting to upgrade.
   */
  public boolean tryUpgrade(long aTime, TimeUnit aUnit) throws InterruptedException {
    return upgradeInternal(true, aTime, aUnit);
  }
  
  /**
   * Downgrades the thread's hold on the lock. This allows other reader threads
   * to acquire the lock, if the thread has no unmatched upgrades and does not
   * hold a write lock.
   * 
   * @throws IllegalMonitorStateException
   *         if the thread has not upgraded.
   */
  public void downgrade() {
    ThreadState mState = myThreadState.get();
    if (mState == null) {
      throw new IllegalMonitorStateException("Cannot upgrade without lock");
    }
    if (mState.acquiredReadFirst()) {
      throw new IllegalMonitorStateException("Cannot upgrade or downgrade from read");
    }
    if (mState.isDowngraded()) {
      throw new IllegalMonitorStateException("Cannot downgrade without upgrade");
    }
    mState.downgrade();
    if (mState.isDowngraded()) {
      mySync.downgrade();
    }
  }
  
  /**
   * Returns a new {@link Condition} object which can be used for waiting and
   * notifying other threads. {@linkplain Condition#await() await()} and
   * {@linkplain Condition#signal() signal()} can only be called by a thread
   * holding the lock associated with the condition in write or upgraded mode.
   */
  public Condition newCondition() {
    return mySync.new ConditionObject();
  }
  
  private boolean lockInternal(Mode aMode, boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
    switch (aMode) {
      case READ: return readLockInternal(aInterruptible, aTime, aUnit);
      case UPGRADABLE: return upgradableLockInternal(aInterruptible, aTime, aUnit);
      case WRITE: return writeLockInternal(aInterruptible, aTime, aUnit);
      default: throw new AssertionError();
    }
  }
  
  private boolean readLockInternal(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
    ThreadState mState = myThreadState.get();
    if (mState == null) {
      if (!mySync.acquireShared(aInterruptible, aTime, aUnit)) return false;
      mState = new ThreadState(true);
      myThreadState.set(mState);
    }
    mState.incrementRead();
    return true;
  }
  
  private boolean writeLockInternal(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
    ThreadState mState = myThreadState.get();
    if (mState != null && mState.acquiredReadFirst()) {
      throw new IllegalMonitorStateException("Cannot upgrade from read");
    }
    if (mState == null) {
      if (!mySync.acquire(false, aInterruptible, aTime, aUnit)) return false;
      mState = new ThreadState(false);
      myThreadState.set(mState);
    } else if (mState.isDowngraded()) {
      if (!mySync.acquire(true, aInterruptible, aTime, aUnit)) return false;
    }
    mState.incrementWrite();
    return true;
  }
  
  private boolean upgradableLockInternal(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
    ThreadState mState = myThreadState.get();
    if (mState != null && mState.acquiredReadFirst()) {
      throw new IllegalMonitorStateException("Cannot upgrade from read");
    }
    if (mState == null) {
      mState = new ThreadState(false);
      if (!mySync.acquireShared(aInterruptible, aTime, aUnit)) return false;
      myThreadState.set(mState);
    }
    mState.incrementUpgradable();
    return true;
  }
  
  private boolean upgradeInternal(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
    ThreadState mState = myThreadState.get();
    if (mState == null) {
      throw new IllegalMonitorStateException("Cannot upgrade without lock");
    }
    if (mState.acquiredReadFirst()) {
      throw new IllegalMonitorStateException("Cannot upgrade from read");
    }
    if (mState.isDowngraded()) {
      if (!mySync.acquire(true, aInterruptible, aTime, aUnit)) return false;
    }
    mState.upgrade();
    return true;
  }
}
