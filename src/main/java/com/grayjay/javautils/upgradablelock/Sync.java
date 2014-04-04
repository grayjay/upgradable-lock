package com.grayjay.javautils.upgradablelock;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import com.grayjay.javautils.upgradablelock.UpgradableLock.Mode;
import com.grayjay.javautils.upgradablelock.UpgradableLock.TooManyHoldsException;


final class Sync {
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

  /**
   * Timeout value used internally when the timeout argument is negative.
   * This prevents collision with the other time constants.
   */
  static final long MIN_TIMEOUT = -1L;

  /**
   * Timeout value used internally after a call to tryLock(Mode) or tryUpgrade()
   * where the thread does not wait.
   */
  static final long NO_WAIT = -2L;

  /**
   * Timeout value used internally after a call to lock(Mode) or upgrade(),
   * where the lock attempt never times out.
   */
  static final long NO_TIMEOUT = -3L;

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
    if (!myIsFair || myQueue.isEmpty()) {
      if (tryLock(aMode)) return true;
    }
    if (aTime == NO_WAIT) return false;
    return enqueueAndLock(aMode, aInterruptible, aTime, aUnit);
  }

  boolean upgrade(boolean aInterruptible, long aTime, TimeUnit aUnit) throws InterruptedException {
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
      case READ: {
        int mState;
        int mNewState;
        do {
          mState = myState.get();
          int mReadHolds = getReadHolds(mState);
          mNewState = setReadHolds(mState, mReadHolds - 1);
        } while (!myState.compareAndSet(mState, mNewState));
        break;
      }
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
