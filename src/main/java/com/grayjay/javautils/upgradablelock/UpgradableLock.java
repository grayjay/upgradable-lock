package com.grayjay.javautils.upgradablelock;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * A reentrant read-write lock allowing at most one designated upgradable thread
 * that can switch between reading and writing. Other readers can acquire the
 * lock while the thread with the upgradable lock is downgraded.
 * <p>
 * A thread can initially acquire the lock in any of three modes: read,
 * upgradable, and write. A thread acquiring an upgradable lock starts in the
 * downgraded state. All locks and unlocks are nested. This means that a thread
 * cannot acquire a write lock, then a read lock, and then release the write
 * lock without releasing the read lock. Calls to downgrade must be matched by
 * calls to upgrade. Calls to upgrade and downgrade can be interleaved with
 * calls to lock and unlock in any order, as long as the thread has an
 * upgradable or write lock when upgrading or downgrading. A thread with only a
 * read lock cannot acquire an upgradable or write lock. Any thread with an
 * upgradable or write lock can acquire the lock again in any of the three
 * modes. Acquiring a read lock after an upgradable or write lock has no effect,
 * though it still must be released.
 * <p>
 * This lock uses fair queuing by default, with holds given in
 * first-in-first-out order. The lock can also be constructed with non-fair
 * behavior. A non-fair lock allows a running thread to try acquiring the lock
 * without waiting in the queue, in cases where the lack of fairness could not
 * allow reader threads to starve writer threads indefinitely.
 * <p>
 * This class is {@linkplain Serializable serializable}. It is always
 * deserialized in the fully unlocked state.
 * 
 * @serial exclude
 */
public final class UpgradableLock implements Serializable {
  /*
   * A thread has one of four possible exclusion levels at any point in time:
   * 1. No lock - no exclusion of other threads
   * 2. Read lock - exclusion of level 4 threads
   * 3. Downgraded upgradable lock - exclusion of level 3 and 4 threads
   * 4. Write lock or upgraded upgradable lock - exclusion of all other threads
   * 
   * Each operation on the lock involves three steps:
   * 
   * 1. The thread reads a thread-local variable, myThreadState, to determine
   * the type of holds that it already has. It determines whether the current
   * operation involves a change in the thread's exclusion level. For example,
   * acquiring a write lock after acquiring an upgradable lock involves a change
   * from level 3 to 4. However, recursive calls to lock with the same mode do
   * not involve a change in exclusion level.
   * 
   * 2. If the thread needs to change its exclusion level, it calls the
   * corresponding method on an internal, non-reentrant, upgradable lock.  The
   * internal lock is stored in variable mySync. mySync keeps a count of threads
   * with each exclusion level from 2 to 4. Those counts are the authority on
   * which threads hold the lock, and threads compete to update them. mySync
   * uses a queue to store threads waiting for the lock.
   * 
   * 3. If the thread either does not need to change its exclusion level, or it
   * succeeds in changing its exclusion level in mySync, the operation
   * succeeds. The thread then updates the thread-local variable to reflect the
   * change in its number or type of holds. If the operation fails, the thread
   * simply returns false.
   */
  
  private static final long serialVersionUID = 0L;
  
  /**
   * Timeout value used internally when the timeout argument is negative.
   * This prevents collision with the other time constants.
   */
  private static final long MIN_TIMEOUT = -1L;
  
  /**
   * Timeout value used internally after a call to tryLock(Mode) or tryUpgrade()
   * where the thread does not wait.
   */
  private static final long NO_WAIT = -2L;
  
  /**
   * Timeout value used internally after a call to lock(Mode) or upgrade(),
   * where the lock attempt never times out.
   */
  private static final long NO_TIMEOUT = -3L;
  
  private final Sync mySync;
  private final ThreadLocal<ThreadState> myThreadState = new ThreadLocal<ThreadState>() {
    @Override
    protected ThreadState initialValue() {
      return ThreadState.newState();
    }
  };
  
  /**
   * Constructs a fair {@code UpgradableLock}. This is the same as calling
   * {@code UpgradableLock(true)}.
   */
  public UpgradableLock() {
    this(true);
  }
  
  /**
   * Constructs an {@code UpgradableLock} with the given choice of fairness.
   * 
   * @param aIsFair whether this lock should always use first-in-first-out
   * ordering.
   */
  public UpgradableLock(boolean aIsFair) {
    mySync = new Sync(aIsFair);
  }
  
  /**
   * Modes used to acquire an {@link UpgradableLock}.
   */
  public static enum Mode {
    /**
     * Locking mode that allows concurrent access by other threads locking in
     * {@code READ} mode or downgraded {@linkplain Mode#UPGRADABLE UPGRADABLE}
     * mode.
     */
    READ,
    
    /**
     * Locking mode that allows upgrading and downgrading. All other threads are
     * excluded when upgraded, but threads using {@linkplain Mode#READ READ}
     * mode are allowed when downgraded.
     */
    UPGRADABLE,
    
    /**
     * Locking mode that excludes all other threads.
     */
    WRITE
  }

  /**
   * Thrown when a thread attempts to acquire or upgrade the lock when the lock
   * already has the maximum number of holds.
   */
  public static final class TooManyHoldsException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    TooManyHoldsException(String aMessage) {
      super(aMessage);
    }
  }
  
  /**
   * Lock state that applies to the current thread. It stores the numbers
   * and types of holds that the thread currently has.
   */
  private static final class ThreadState {
    /**
     * The value of myFirstWriteHold when the thread has no write holds.
     */
    private static final int NO_WRITE_HOLDS = -1;
    
    /**
     * Type of the first hold acquired by this thread, or FirstHold.NONE if the
     * thread does not yet hold the lock. The first hold determines what types
     * of reentrant holds are allowed.
     */
    private final FirstHold myFirstHold;
    
    /**
     * The number of reentrant upgrades.
     */
    private final int myUpgradeCount;
    
    /**
     * The number of reentrant lock holds of any type.
     */
    private final int myHoldCount;
    
    /**
     * The number of reentrant holds at the time that the thread first acquired
     * the lock in write mode. If the thread does not have a write hold, the
     * value is equal to NO_WRITE_HOLDS.
     */
    private final int myFirstWriteHold;
    
    private static enum FirstHold {
      NONE,
      READ,
      UPGRADABLE,
      WRITE;
    }

    private static final ThreadState NEW = new ThreadState(FirstHold.NONE, 0, 0, NO_WRITE_HOLDS);
    
    static ThreadState newState() {
      // reuse instance for efficiency
      return NEW;
    }
    
    private ThreadState(FirstHold aFirstHold, int aUpgrades, int aHolds, int aFirstWrite) {
      myFirstHold = aFirstHold;
      myUpgradeCount = aUpgrades;
      myHoldCount = aHolds;
      myFirstWriteHold = aFirstWrite;
    }
    
    Mode getFirstHold() {
      switch (myFirstHold) {
        case NONE: throw new IllegalArgumentException("No hold yet");
        case READ: return Mode.READ;
        case UPGRADABLE: return Mode.UPGRADABLE;
        case WRITE: return Mode.WRITE;
        default: throw new AssertionError();
      }
    }

    boolean acquiredReadFirst() {
      return myFirstHold == FirstHold.READ;
    }
    
    boolean isUnlocked() {
      return myHoldCount == 0;
    }
    
    /**
     * Returns true if the thread holds only a read lock or a downgraded
     * upgradable lock.
     */
    boolean canWrite() {
      return myFirstHold == FirstHold.WRITE ||
          myFirstHold == FirstHold.UPGRADABLE &&
          (myUpgradeCount > 0 || myFirstWriteHold != NO_WRITE_HOLDS);
    }

    boolean isDowngraded() {
      return myUpgradeCount == 0;
    }

    ThreadState incrementWrite() {
      FirstHold mFirst = myFirstHold == FirstHold.NONE ? FirstHold.WRITE : myFirstHold;
      int mNewHolds = incrementHolds();
      int mFirstWrite = (myFirstWriteHold == NO_WRITE_HOLDS) ? mNewHolds : myFirstWriteHold;
      return new ThreadState(mFirst, myUpgradeCount, mNewHolds, mFirstWrite);
    }

    ThreadState incrementUpgradable() {
      FirstHold mFirst = myFirstHold == FirstHold.NONE ? FirstHold.UPGRADABLE : myFirstHold;
      int mNewHolds = incrementHolds();
      return new ThreadState(mFirst, myUpgradeCount, mNewHolds, myFirstWriteHold);
    }

    ThreadState incrementRead() {
      FirstHold mFirst = myFirstHold == FirstHold.NONE ? FirstHold.READ : myFirstHold;
      int mNewHolds = incrementHolds();
      return new ThreadState(mFirst, myUpgradeCount, mNewHolds, myFirstWriteHold);
    }

    ThreadState decrementHolds() {
      int mFirstWrite = (myFirstWriteHold == myHoldCount) ? NO_WRITE_HOLDS : myFirstWriteHold;
      int mNewHolds = myHoldCount - 1;
      return new ThreadState(myFirstHold, myUpgradeCount, mNewHolds, mFirstWrite);
    }

    ThreadState upgrade() {
      if (myUpgradeCount == Integer.MAX_VALUE) {
        throw new TooManyHoldsException("Too many upgrades");
      }
      return new ThreadState(myFirstHold, myUpgradeCount + 1, myHoldCount, myFirstWriteHold);
    }

    ThreadState downgrade() {
      return new ThreadState(myFirstHold, myUpgradeCount - 1, myHoldCount, myFirstWriteHold);
    }
    
    private int incrementHolds() {
      if (myHoldCount == Integer.MAX_VALUE) {
        throw new TooManyHoldsException("Too many holds");
      }
      return myHoldCount + 1;
    }
  }

  private static final class Sync {
    /*
     * This class uses the int state in myState to maintain 3 counts:
     * 
     * - threads with read locks
     * - threads with downgraded upgradable locks
     * - threads with write/upgraded locks
     * 
     * It does not keep track of the number of holds per thread or which holds
     * allow downgrading. The lowest bit stores the number of writer threads,
     * the next bit stores the number of downgraded threads, and the rest of
     * the state stores the number of reader threads.
     * 
     * Threads wait in a queue to acquire the lock in any of the three modes.
     * If the lock is held by a thread in upgradable mode, that thread waits to
     * upgrade in the variable myUpgrading.
     */
    
    private static final int READ_SHIFT = 2;
    private static final int WRITE_BIT = 1;
    private static final int UPGRADABLE_BIT = 2;
    private static final int MAX_READ_HOLDS = Integer.MAX_VALUE >>> READ_SHIFT;

    private final boolean myIsFair;
    private final AtomicInteger myState = new AtomicInteger(calcState(false, false, 0));
    private final Queue<Node> myQueue = new ConcurrentLinkedQueue<>();
    private volatile Thread myUpgrading;
    
    Sync(boolean aIsFair) {
      myIsFair = aIsFair;
    }
    
    private static final class Node {
      final Mode myMode;
      final Thread myThread;
      
      Node(Mode aMode, Thread aThread) {
        myMode = aMode;
        myThread = aThread;
      }
    }
    
    boolean lock(Mode aMode, boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
      if (aInterruptible && Thread.interrupted()) {
        throw new InterruptedException();
      }
      if (!myIsFair || myQueue.isEmpty()) {
        if (tryLock(aMode)) return true;
      }
      if (aTime == NO_WAIT) return false;
      return enqueueAndLock(aMode, aInterruptible, aTime, aUnit);
    }

    boolean upgrade(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
      if (aInterruptible && Thread.interrupted()) {
        throw new InterruptedException();
      }
      if (tryUpgrade()) return true;
      if (aTime == NO_WAIT) return false;
      return enqueueAndUpgrade(aInterruptible, aTime, aUnit);
    }
    
    void unlock(Mode aMode) {
      switch (aMode) {
        case WRITE:
          myState.set(calcState(false, false, 0));
          break;
        case UPGRADABLE: {
          int mState;
          int mNewState;
          do {
            mState = myState.get();
            mNewState = setUpgradableHold(mState, false);
          } while (!myState.compareAndSet(mState, mNewState));
          break;
        }
        case READ: 
          int mState;
          int mNewState;
          do {
            mState = myState.get();
            int mReadHolds = getReadHolds(mState);
            mNewState = setReadHolds(mState, mReadHolds - 1);
          } while (!myState.compareAndSet(mState, mNewState));
          break;
        default: throw new AssertionError();
      }
      tryUnparkNext();
    }
    
    void downgrade() {
      myState.set(calcState(false, true, 0));
      tryUnparkNext();
    }
    
    /**
     * Tries to acquire the lock as long as the current state allows, and
     * returns true on success.
     */
    private boolean tryLock(Mode aMode) {
      int mState;
      int mNewState;
      do {
        mState = myState.get();
        if (!canLock(aMode, mState)) return false;
        switch (aMode) {
          case READ:
            int mReadHolds = getReadHolds(mState);
            if (mReadHolds == MAX_READ_HOLDS) {
              throw new TooManyHoldsException("Too many reader threads");
            }
            mNewState = setReadHolds(mState, mReadHolds + 1);
            break;
          case UPGRADABLE:
            mNewState = setUpgradableHold(mState, true);
            break;
          case WRITE:
            mNewState = calcState(true, false, 0);
            break;
          default: throw new AssertionError();
        }
      } while (!myState.compareAndSet(mState, mNewState));
      return true;
    }

    private boolean tryUpgrade() {
      int mState = myState.get();
      if (!canUpgrade(mState)) return false;
      int mNewState = calcState(true, false, 0);
      return myState.compareAndSet(mState, mNewState);
    }

    private boolean enqueueAndLock(Mode aMode, boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
      Thread mCurrent = Thread.currentThread();
      Node mNode = new Node(aMode, mCurrent);
      myQueue.add(mNode);
      long mDeadline = System.nanoTime() + aUnit.toNanos(aTime);
      boolean mInterrupted = false;
      while (myQueue.peek() != mNode || !tryLock(aMode)) {
        if (aTime != NO_TIMEOUT) {
          parkUntil(mDeadline);
          if (System.nanoTime() > mDeadline) {
            removeFailedNodeAndSignal(mNode);
            return false;
          }
        } else LockSupport.park(this);
        if (Thread.interrupted()) {
          if (aInterruptible) {
            removeFailedNodeAndSignal(mNode);
            throw new InterruptedException();
          } else mInterrupted = true;
        }
      }
      myQueue.remove();
      tryUnparkNext();
      if (mInterrupted) {
        mCurrent.interrupt();
      }
      return true;
    }
    
    private void removeFailedNodeAndSignal(Node aNode) {
      myQueue.remove(aNode);
      tryUnparkNext();
    }

    private boolean enqueueAndUpgrade(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
      Thread mCurrent = Thread.currentThread();
      myUpgrading = mCurrent;
      long mDeadline = System.nanoTime() + aUnit.toNanos(aTime);
      boolean mInterrupted = false;
      while (!tryUpgrade()) {
        if (aTime != NO_TIMEOUT) {
          parkUntil(mDeadline);
          if (System.nanoTime() > mDeadline) {
            myUpgrading = null;
            tryUnparkNext();
            return false;
          }
        } else LockSupport.park(this);
        if (Thread.interrupted()) {
          if (aInterruptible) {
            myUpgrading = null;
            tryUnparkNext();
            throw new InterruptedException();
          } else mInterrupted = true;
        }
      }
      myUpgrading = null;
      if (mInterrupted) {
        mCurrent.interrupt();
      }
      return true;
    }
    
    private void parkUntil(long aDeadlineNanos) {
      long mStart = System.nanoTime();
      LockSupport.parkNanos(this, aDeadlineNanos - mStart);
    }
    
    /**
     * Unparks the next thread only if the current state allows that thread to
     * acquire the lock. Preference is given to upgrades.
     */
    private void tryUnparkNext() {
      int mState = myState.get();
      if (hasWriteHold(mState)) return;
      if (canUpgrade(mState)) {
        Thread mUpgrading = myUpgrading;
        if (mUpgrading != null) {
          LockSupport.unpark(mUpgrading);
          return;
        }
      }
      Node mNext = myQueue.peek();
      if (mNext != null && canLock(mNext.myMode, mState)) {
        LockSupport.unpark(mNext.myThread);
      }
    }
    
    private boolean canLock(Mode aMode, int aState) {
      if (hasWriteHold(aState)) return false;
      switch (aMode) {
        case READ:
          // prevent starvation of writer threads
          return !nextThreadIsWriter();
        case UPGRADABLE:
          return !hasUpgradableHold(aState);
        case WRITE:
          return !hasUpgradableHold(aState) && getReadHolds(aState) == 0;
        default: throw new AssertionError();
      }
    }
    
    private static boolean canUpgrade(int aState) {
      return aState == calcState(false, true, 0);
    }
    
    private boolean nextThreadIsWriter() {
      if (myUpgrading != null) return true;
      Node mNext = myQueue.peek();
      return mNext != null && mNext.myMode == Mode.WRITE;
    }
    
    private static boolean hasWriteHold(int aState) {
      return (aState & WRITE_BIT) != 0;
    }
    
    private static boolean hasUpgradableHold(int aState) {
      return (aState & UPGRADABLE_BIT) != 0;
    }
    
    private static int getReadHolds(int aState) {
      return aState >>> READ_SHIFT;
    }
    
    private static int setUpgradableHold(int aState, boolean aUpgradable) {
      return aUpgradable ? aState | UPGRADABLE_BIT : aState & ~UPGRADABLE_BIT;
    }
    
    private static int setReadHolds(int aState, int aReadHolds) {
      return aReadHolds << READ_SHIFT | aState & (WRITE_BIT | UPGRADABLE_BIT);
    }

    private static int calcState(boolean aWrite, boolean aUpgradable, int aRead) {
      int mState = aRead << READ_SHIFT;
      if (aUpgradable) mState |= UPGRADABLE_BIT;
      if (aWrite) mState |= WRITE_BIT;
      return mState;
    }
    
    @Override
    public String toString() {
      int mState = myState.get();
      String mHoldsMessage;
      if (hasWriteHold(mState)) {
        mHoldsMessage = "1 write/upgraded thread";
      } else {
        boolean mUpgradableHold = hasUpgradableHold(mState);
        int mReadHolds = getReadHolds(mState);
        if (mReadHolds == 0 && !mUpgradableHold) mHoldsMessage = "unlocked";
        else {
          mHoldsMessage = "";
          if (mUpgradableHold) mHoldsMessage += "1 downgraded thread";
          if (mReadHolds > 0) {
            if (mUpgradableHold) mHoldsMessage += ", ";
            mHoldsMessage += mReadHolds + " read thread";
            if (mReadHolds > 1) mHoldsMessage += "s";
          }
        }
      }
      String mFairness = myIsFair ? "fair" : "non-fair";
      return "[" + mFairness + ", " + mHoldsMessage + "]";
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
    Objects.requireNonNull(aUnit, "Null time unit");
    long mTime = boundTimeout(aTime);
    return lockInternal(aMode, true, mTime, aUnit);
  }
  
  /**
   * Releases the thread's latest hold on the lock.
   * 
   * @throws IllegalMonitorStateException if the thread does not hold the lock.
   */
  public void unlock() {
    ThreadState mOld = myThreadState.get();
    if (mOld.isUnlocked()) {
      throw new IllegalMonitorStateException("Cannot unlock lock that was not held");
    }
    boolean mWasWrite = mOld.canWrite();
    ThreadState mNew = mOld.decrementHolds();
    if (mNew.isUnlocked()) {
      Mode mToRelease = mWasWrite ? Mode.WRITE : mOld.getFirstHold();
      mySync.unlock(mToRelease);
      myThreadState.remove();
      return;
    } else if (mWasWrite && !mNew.canWrite()) {
      mySync.downgrade();
    }
    myThreadState.set(mNew);
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
    Objects.requireNonNull(aUnit, "Null time unit");
    long mTime = boundTimeout(aTime);
    return upgradeInternal(true, mTime, aUnit);
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
    ThreadState mOld = myThreadState.get();
    if (mOld.isUnlocked()) {
      throw new IllegalMonitorStateException("Cannot downgrade without lock");
    }
    if (mOld.acquiredReadFirst()) {
      throw new IllegalMonitorStateException("Cannot upgrade or downgrade from read");
    }
    if (mOld.isDowngraded()) {
      throw new IllegalMonitorStateException("Cannot downgrade without upgrade");
    }
    ThreadState mNew = mOld.downgrade();
    if (!mNew.canWrite()) {
      mySync.downgrade();
    }
    myThreadState.set(mNew);
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
    ThreadState mOld = myThreadState.get();
    ThreadState mNew = mOld.incrementRead();
    if (mOld.isUnlocked()) {
      if (!mySync.lock(Mode.READ, aInterruptible, aTime, aUnit)) return false;
    }
    myThreadState.set(mNew);
    return true;
  }
  
  private boolean writeLockInternal(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
    ThreadState mOld = myThreadState.get();
    if (mOld.acquiredReadFirst()) {
      throw new IllegalMonitorStateException("Cannot upgrade from read");
    }
    ThreadState mNew = mOld.incrementWrite();
    if (mOld.isUnlocked()) {
      if (!mySync.lock(Mode.WRITE, aInterruptible, aTime, aUnit)) return false;
    } else if (!mOld.canWrite()) {
      if (!mySync.upgrade(aInterruptible, aTime, aUnit)) return false;
    }
    myThreadState.set(mNew);
    return true;
  }
  
  private boolean upgradableLockInternal(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
    ThreadState mOld = myThreadState.get();
    if (mOld.acquiredReadFirst()) {
      throw new IllegalMonitorStateException("Cannot upgrade from read");
    }
    ThreadState mNew = mOld.incrementUpgradable();
    if (mOld.isUnlocked()) {
      if (!mySync.lock(Mode.UPGRADABLE, aInterruptible, aTime, aUnit)) return false;
    }
    myThreadState.set(mNew);
    return true;
  }
  
  private boolean upgradeInternal(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
    ThreadState mOld = myThreadState.get();
    if (mOld.isUnlocked()) {
      throw new IllegalMonitorStateException("Cannot upgrade without lock");
    }
    if (mOld.acquiredReadFirst()) {
      throw new IllegalMonitorStateException("Cannot upgrade from read");
    }
    ThreadState mNew = mOld.upgrade();
    if (!mOld.canWrite()) {
      if (!mySync.upgrade(aInterruptible, aTime, aUnit)) return false;
    }
    myThreadState.set(mNew);
    return true;
  }
  
  private static long boundTimeout(long aTime) {
    return aTime < 0 ? MIN_TIMEOUT : aTime;
  }
  
  @Override
  public String toString() {
    return "UpgradableLock" + mySync.toString();
  }
  
  private Object writeReplace() {
    return new SerializationProxy();
  }
  
  private void readObject(ObjectInputStream aOis) throws InvalidObjectException {
    throw new InvalidObjectException("Expecting serialization proxy");
  }
  
  /*
   * A new upgradable lock must be created after deserialization to allow all
   * fields to be final while avoiding serializing the thread local state. A
   * serialization proxy makes this easier.
   */
  /**
   * @serial include
   */
  private static final class SerializationProxy implements Serializable {
    private static final long serialVersionUID = 0L;
    
    private Object readResolve() {
      return new UpgradableLock();
    }
  }
}
