package concurrency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Condition;

import org.junit.*;
import org.junit.rules.Timeout;

import concurrency.UpgradableLock.Mode;

public class UpgradableLockTests {
  private static int MAX_TEST_LENGTH_MILLIS = 5000;
  private static long MAX_WAIT_FOR_LOCK_MILLIS = 10;
  
  private UpgradableLock myLock;
  
  @Rule
  public Timeout mTimeout = new Timeout(MAX_TEST_LENGTH_MILLIS);
  
  @Before
  public void setup() {
    myLock = new UpgradableLock();
  }

  @Test
  public void testWriteLock() throws InterruptedException {
    myLock.lock(Mode.WRITE);
    myLock.lock(Mode.READ);
    assertTrue(hasWriter());
    myLock.unlock();
    assertTrue(hasWriter());
    myLock.unlock();
    assertTrue(isUnlocked());
  }

  @Test
  public void testUpgrading() throws InterruptedException {
    myLock.lock(Mode.UPGRADABLE);
    assertTrue(hasReaders());
    myLock.lock(Mode.READ);
    assertTrue(hasReaders());
    myLock.upgrade();
    assertTrue(hasWriter());
    myLock.downgrade();
    assertTrue(hasReaders());
    myLock.lock(Mode.WRITE);
    assertTrue(hasWriter());
    myLock.unlock();
    assertTrue(hasReaders());
    myLock.unlock();
    assertTrue(hasReaders());
    myLock.unlock();
    assertTrue(isUnlocked());
  }

  @Test
  public void testReadLock() throws InterruptedException {
    myLock.lock(Mode.READ);
    assertTrue(hasReaders());
    myLock.lock(Mode.READ);
    assertTrue(hasReaders());
    myLock.unlock();
    assertTrue(hasReaders());
    myLock.unlock();
    assertTrue(isUnlocked());
  }
  
  @Test
  public void clearUpgradesAfterFullUnlock() throws InterruptedException {
    myLock.lock(Mode.UPGRADABLE);
    myLock.upgrade();
    assertTrue(hasWriter());
    myLock.unlock();
    myLock.lock(Mode.UPGRADABLE);
    assertTrue(hasReaders());
  }
  
  @Test
  public void keepUpgradesAfterReleasingWriteLock() throws InterruptedException {
    myLock.lock(Mode.UPGRADABLE);
    myLock.lock(Mode.WRITE);
    myLock.upgrade();
    assertTrue(hasWriter());
    myLock.unlock();
    assertTrue(hasWriter());
    myLock.downgrade();
    assertTrue(hasReaders());
  }
  
  @Test
  public void testTimeout() throws InterruptedException {
    lockPermanently(Mode.WRITE);
    for (Mode mMode : Mode.values()) {
      boolean mSuccess = myLock.tryLock(mMode, 100, TimeUnit.MICROSECONDS);
      assertFalse(mSuccess);
    }
  }
  
  @Test
  public void testTryLockWhenAvailable() throws InterruptedException {
    for (Mode mMode : Mode.values()) {
      assertTrue(myLock.tryLock(mMode));
      myLock.unlock();
      assertTrue(isUnlocked());
    }
  }
  
  @Test
  public void testTryLockWhenNotAvailable() throws InterruptedException {
    lockPermanently(Mode.WRITE);
    for (Mode mMode : Mode.values()) {
      assertFalse(myLock.tryLock(mMode));
    }
  }

  @Test
  public void testInterruption() throws InterruptedException {
    final AtomicBoolean mInterrupted = new AtomicBoolean();
    Thread mThread = new Thread() {
      @Override
      public void run() {
        try {
          myLock.lockInterruptibly(Mode.UPGRADABLE);
        } catch (InterruptedException e) {
          mInterrupted.set(true);
        }
      }
    };
    lockPermanently(Mode.WRITE);
    mThread.start();
    Thread.sleep(MAX_WAIT_FOR_LOCK_MILLIS);
    mThread.interrupt();
    mThread.join();
    assertTrue(mInterrupted.get());
  }
  
  @Test
  public void retainInterruptedStatusWithTryLock() throws InterruptedException {
    Thread mCurrent = Thread.currentThread();
    mCurrent.interrupt();
    assertTrue(myLock.tryLock(Mode.WRITE));
    myLock.unlock();
    assertTrue(Thread.interrupted());
    lockPermanently(Mode.READ);
    mCurrent.interrupt();
    assertFalse(myLock.tryLock(Mode.WRITE));
    assertTrue(Thread.interrupted());
  }
  
  /*
   * Several threads with read locks and one thread with an upgradable lock
   * wait on a barrier.
   */
  @Test
  public void allowConcurrentReadAndUpgradableAccess() throws InterruptedException {
    int mNThreads = 4;
    final CyclicBarrier mBarrier = new CyclicBarrier(mNThreads);
    final AtomicReference<Throwable> mError = new AtomicReference<Throwable>();
    ExecutorService mPool = Executors.newCachedThreadPool();
    for (int i = 0; i < mNThreads - 1; i++) {
      mPool.execute(newBarrierTask(mBarrier, Mode.READ, mError));
    }
    mPool.execute(newBarrierTask(mBarrier, Mode.UPGRADABLE, mError));
    mPool.shutdown();
    assertTrue(mPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS));
    assertEquals(null, mError.get());
    assertTrue(isUnlocked());
  }
  
  private Runnable newBarrierTask(
      final CyclicBarrier aBarrier,
      final Mode aMode,
      final AtomicReference<Throwable> aError) {
    return new Runnable() {
      @Override
      public void run() {
        myLock.lock(aMode);
        try {
          aBarrier.await();
        } catch (Exception e) {
          aError.set(e);
        } finally {
          myLock.unlock();
        }
      }
    };
  }

  /*
   * Threads 0 - n wait on a condition for a counter to reach their index
   * numbers. Then they increment the counter and notify the other threads. The
   * counter starts at zero to allow the first thread to proceed without
   * waiting.
   */
  @Test
  public void testCondition() throws InterruptedException {
    final Condition mIncremented = myLock.newCondition();
    final AtomicInteger mCounter = new AtomicInteger();
    ExecutorService mPool = Executors.newCachedThreadPool();
    int mNThreads = 10;
    for (int i = 0; i < mNThreads; i++) {
      mPool.execute(newWaitTask(i, mCounter, mIncremented));
    }
    mPool.shutdown();
    mPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    assertEquals(mNThreads, mCounter.get());
  }
  
  private Runnable newWaitTask(
      final int aExpectedCount,
      final AtomicInteger aCounter,
      final Condition aIncremented) {
    return new Runnable() {
      @Override
      public void run() {
        myLock.lock(Mode.WRITE);
        try {
          while (aCounter.get() != aExpectedCount) {
            aIncremented.await();
          }
          aCounter.incrementAndGet();
          aIncremented.signalAll();
        } catch (InterruptedException e) {
          // return
        } finally {
          myLock.unlock();
        }
      }
    };
  }
  
  @Test
  public void deserializeInUnlockedState() throws IOException, InterruptedException, ClassNotFoundException {
    lockPermanently(Mode.WRITE);
    byte[] mSerializedLock = serialize(myLock);
    assertTrue(hasWriter());
    myLock = (UpgradableLock) deserialize(mSerializedLock);
    assertTrue(isUnlocked());
  }
  
  private static byte[] serialize(Serializable aValue) throws IOException {
    ByteArrayOutputStream mOS = new ByteArrayOutputStream();
    ObjectOutputStream mOOS = new ObjectOutputStream(mOS);
    mOOS.writeObject(aValue);
    return mOS.toByteArray();
  }
  
  private static Serializable deserialize(byte[] aSerialized) throws IOException, ClassNotFoundException {
    InputStream mIS = new ByteArrayInputStream(aSerialized);
    ObjectInputStream mOIS = new ObjectInputStream(mIS);
    return (Serializable) mOIS.readObject();
  }
  
  @Test(expected=IllegalMonitorStateException.class)
  public void preventWaitingWithoutWriteLock() throws InterruptedException {
    myLock.lock(Mode.READ);
    Condition mCondition = myLock.newCondition();
    mCondition.await();
  }

  @Test(expected=IllegalMonitorStateException.class)
  public void preventSignallingWithoutWriteLock() {
    myLock.lock(Mode.UPGRADABLE);
    Condition mCondition = myLock.newCondition();
    mCondition.signal();
  }

  /*
   * One thread tries to acquire the write lock multiple times while several
   * threads use a counter to try to trade off acquiring the read lock.
   */
  @Test
  public void preventWriterStarvation() throws InterruptedException {
    Mode[] mModes = {Mode.WRITE, Mode.UPGRADABLE};
    for (Mode mMode : mModes) {
      final AtomicInteger mCounter = new AtomicInteger();
      ExecutorService mPool = Executors.newCachedThreadPool();
      for (int i = 0; i < 3; i++) {
        mPool.execute(new Runnable() {
          @Override
          public void run() {
            try {
              while (true) {
                myLock.lockInterruptibly(Mode.READ);
                try {
                  int mStartCount = mCounter.incrementAndGet();
                  long mEndTime = System.nanoTime() + 5000 * 1000;
                  while (mCounter.get() == mStartCount && System.nanoTime() < mEndTime) {
                    Thread.sleep(0, 1000);
                  }
                } finally {
                  myLock.unlock();
                }
              }
            } catch (InterruptedException e) {
              // return
            }
          }
        });
      }
      for (int i = 0; i < 5; i++) {
        Thread.sleep(MAX_WAIT_FOR_LOCK_MILLIS);
        myLock.lock(mMode);
        myLock.upgrade();
        myLock.unlock();
      }
      mPool.shutdownNow();
      mPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
      assertTrue(isUnlocked());
    }
  }
  
  @Test
  public void releaseWriteWithWaitingThreads() throws InterruptedException {
    Mode[] mModes = {Mode.UPGRADABLE, Mode.WRITE};
    for (Mode mMode : mModes) {
      Thread mThread = new Thread() {
        @Override
        public void run() {
          myLock.lock(Mode.READ);
          myLock.unlock();
        }
      };
      myLock.lock(mMode);
      myLock.upgrade();
      mThread.start();
      Thread.sleep(MAX_WAIT_FOR_LOCK_MILLIS);
      myLock.downgrade();
      if (mMode == Mode.WRITE) myLock.unlock();
      mThread.join();
      if (mMode == Mode.UPGRADABLE) myLock.unlock();
      assertTrue(isUnlocked());
    }
  }
  
  @Test
  public void releaseReadWithWaitingThreads() throws InterruptedException {
    Mode[] mUnlockModes = {Mode.UPGRADABLE, Mode.READ};
    Mode[] mLockModes = {Mode.UPGRADABLE, Mode.WRITE};
    for (Mode mUnlockMode : mUnlockModes) {
      for (final Mode mLockMode : mLockModes) {
        Thread mThread = new Thread() {
          @Override
          public void run() {
            myLock.lock(mLockMode);
            myLock.upgrade();
            myLock.unlock();
          }
        };
        myLock.lock(mUnlockMode);
        mThread.start();
        Thread.sleep(MAX_WAIT_FOR_LOCK_MILLIS);
        myLock.unlock();
        mThread.join();
        assertTrue(isUnlocked());
      }
    }
  }

  @Test(expected=IllegalMonitorStateException.class)
  public void preventLockingWriteAfterRead() throws InterruptedException {
    myLock.lock(Mode.READ);
    try {
      myLock.lock(Mode.WRITE);
    } finally {
      assertTrue(hasReaders());
      myLock.unlock();
      assertTrue(isUnlocked());
    }
  }

  @Test(expected=IllegalMonitorStateException.class)
  public void preventUpgradeFromRead() {
    myLock.lock(Mode.READ);
    myLock.upgrade();
  }

  @Test(expected=IllegalMonitorStateException.class)
  public void preventDowngradeWithoutUpgrade() throws InterruptedException {
    myLock.lock(Mode.UPGRADABLE);
    try {
      myLock.downgrade();
    } finally {
      myLock.unlock();
      assertTrue(isUnlocked());
    }
  }

  @Test(expected=IllegalMonitorStateException.class)
  public void preventUnlockWithoutLock() throws InterruptedException {
    try {
      myLock.unlock();
    } finally {
      assertTrue(isUnlocked());
    }
  }
  
  private void lockPermanently(final Mode aMode) throws InterruptedException {
    final AtomicBoolean mSuccess = new AtomicBoolean();
    Thread mThread = new Thread() {
      @Override
      public void run() {
        mSuccess.set(myLock.tryLock(aMode));
      }
    };
    mThread.start();
    mThread.join();
    assertTrue(mSuccess.get());
  }

  private boolean isUnlocked() throws InterruptedException {
    return canLock(Mode.WRITE);
  }
  
  private boolean hasReaders() throws InterruptedException {
    return !canLock(Mode.WRITE) && canLock(Mode.READ);
  }
  
  private boolean hasWriter() throws InterruptedException {
    return !canLock(Mode.READ);
  }

  private boolean canLock(final Mode aMode) throws InterruptedException {
    final AtomicBoolean mSuccess = new AtomicBoolean();
    Thread mThread = new Thread() {
      @Override
      public void run() {
        if (myLock.tryLock(aMode)) {
          mSuccess.set(true);
          myLock.unlock();
        }
      }
    };
    mThread.start();
    mThread.join();
    return mSuccess.get();
  }
}