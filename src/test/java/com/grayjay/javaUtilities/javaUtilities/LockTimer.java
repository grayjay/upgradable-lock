package javaUtilities;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import javaUtilities.UpgradableLock.Mode;


public class LockTimer {
  private static final int N_TRIALS = 10;
  private static final int N_THREADS = Runtime.getRuntime().availableProcessors() + 1;
  private static final int N_LOCKS = 1_000_000;
  
  private int myLockCount = 0;
  
  public static void main(String[] aArgs) throws InterruptedException {
    System.out.println("ReentrantReadWriteLock\tUpgradable\t\tReentrantLock");
    System.out.println();
    System.out.printf("%,d trials with %,d locks per trial (ns/lock)", N_TRIALS, N_LOCKS);
    long mReadWriteNanos = 0;
    long mUpgradableNanos = 0;
    long mReentrantLockNanos = 0;
    for (int i = 0; i < N_TRIALS; i++) {
      long mReadWriteTime = new LockTimer().timeNanos(new ReadWriteLockTest());
      long mUpgradableTime = new LockTimer().timeNanos(new UpgradableLockTest());
      long mReentrantLockTime = new LockTimer().timeNanos(new ReentrantLockTest());
      System.out.println();
      printTrial(mReadWriteTime);
      printTrial(mUpgradableTime);
      printTrial(mReentrantLockTime);
      mReadWriteNanos += mReadWriteTime;
      mUpgradableNanos += mUpgradableTime;
      mReentrantLockNanos += mReentrantLockTime;
    }
    System.out.println();
    System.out.println();
    long[] mTotals = {mReadWriteNanos, mUpgradableNanos, mReentrantLockNanos};
    System.out.println("Average (ns/lock)");
    for (long mTotal : mTotals) {
      printTrial(mTotal / N_TRIALS);
    }
    System.out.println();
    System.out.println();
    System.out.println("Total / ReentrantReadWriteLock:");
    for (long mTotal : mTotals) {
      printRatio(mTotal, mReadWriteNanos);
    }
  }
  
  private static void printTrial(long aNanos) {
    double mAvg = (double) aNanos / N_LOCKS;
    System.out.printf("%.4f\t\t", mAvg);
  }

  private static void printRatio(long aTotal, long aDenom) {
    double mRatio = (double) aTotal / aDenom;
    System.out.printf("%.6f\t\t", mRatio);
  }
  
  private long timeNanos(LockTest aTest) throws InterruptedException {
    assert myLockCount == 0;
    ExecutorService mPool = Executors.newFixedThreadPool(N_THREADS);
    long mStart = System.nanoTime();
    for (int i = 0; i < N_THREADS; i++) {
      Runnable mTask = newTask(aTest);
      mPool.execute(mTask); 
    }
    mPool.shutdown();
    mPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    return System.nanoTime() - mStart;
  }

  private Runnable newTask(final LockTest aTest) {
    return new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < N_LOCKS / N_THREADS; i++) {
          boolean mWrite = i % 3 == 0;
          if (mWrite) {
            aTest.lockWrite();
            write();
            aTest.unlockWrite();
          } else {
            aTest.lockRead();
            read();
            aTest.unlockRead();
          }
        }
      }
    };
  }

  private void read() {
    if (myLockCount == 3210123) {
      System.out.print(" ");
    }
  }

  private void write() {
    myLockCount++;
  }

  private static interface LockTest {
    void lockRead();
    void unlockRead();
    void lockWrite();
    void unlockWrite();
  }
  
  private static final class ReadWriteLockTest implements LockTest {
    private final ReadWriteLock mReadWrite = new ReentrantReadWriteLock();
    private final Lock myReadLock = mReadWrite.readLock();
    private final Lock myWriteLock = mReadWrite.writeLock();
    
    @Override
    public void lockRead() {
      myReadLock.lock();
    }

    @Override
    public void unlockRead() {
      myReadLock.unlock();
    }

    @Override
    public void lockWrite() {
      myWriteLock.lock();
    }

    @Override
    public void unlockWrite() {
      myWriteLock.unlock();
    }
  }
  
  private static final class UpgradableLockTest implements LockTest {
    private final UpgradableLock myUpgradableLock = new UpgradableLock();
    
    @Override
    public void lockRead() {
      myUpgradableLock.lock(Mode.READ);
    }

    @Override
    public void unlockRead() {
      myUpgradableLock.unlock();
    }

    @Override
    public void lockWrite() {
      myUpgradableLock.lock(Mode.WRITE);
    }

    @Override
    public void unlockWrite() {
      myUpgradableLock.unlock();
    }
  }

  private static final class ReentrantLockTest implements LockTest {
    private final Lock myReentrantLock = new ReentrantLock();
    
    @Override
    public void lockRead() {
      myReentrantLock.lock();
    }

    @Override
    public void unlockRead() {
      myReentrantLock.unlock();
    }

    @Override
    public void lockWrite() {
      myReentrantLock.lock();
    }

    @Override
    public void unlockWrite() {
      myReentrantLock.unlock();
    }
  }
}
