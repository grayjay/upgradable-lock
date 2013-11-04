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
    System.out.print("Read/Write\tUpgradable\tReentrant");
    long mReadWriteTotal = 0;
    long mUpgradableTotal = 0;
    long mReentrantLockTotal = 0;
    for (int i = 0; i < N_TRIALS; i++) {
      long mReadWriteTime = new LockTimer().test(new ReadWriteLockTest());
      long mUpgradableTime = new LockTimer().test(new UpgradableLockTest());
      long mReentrantLockTime = new LockTimer().test(new ReentrantLockTest());
      System.out.println();
      System.out.print(mReadWriteTime + "\t\t" + mUpgradableTime + "\t\t" + mReentrantLockTime);
      mReadWriteTotal += mReadWriteTime;
      mUpgradableTotal += mUpgradableTime;
      mReentrantLockTotal += mReentrantLockTime;
    }
    System.out.println();
    System.out.println();
    System.out.println("Average:");
    printAvg(mReadWriteTotal);
    printAvg(mUpgradableTotal);
    printAvg(mReentrantLockTotal);
    System.out.println();
    System.out.println();
    System.out.println("Total / ReadWrite:");
    printRatio(mReadWriteTotal, mReadWriteTotal);
    printRatio(mUpgradableTotal, mReadWriteTotal);
    printRatio(mReentrantLockTotal, mReadWriteTotal);
  }
  
  private static void printAvg(long aTotal) {
//    double mAvg = (double) aTotal / (N_TRIALS * N_LOCKS);
    long mAvg = aTotal / N_TRIALS;
    System.out.print(mAvg + "\t\t");
  }

  private static void printRatio(long aTotal, long aDenom) {
    double mRatio = (double) aTotal / aDenom;
    System.out.printf("%.4f\t\t", mRatio);
  }
  
  private long test(LockTest aTest) throws InterruptedException {
    ExecutorService mPool = Executors.newFixedThreadPool(N_THREADS);
    long mStart = System.nanoTime();
    for (int i = 0; i < N_THREADS; i++) {
      Runnable mTask = newTask(aTest);
      mPool.execute(mTask); 
    }
    mPool.shutdown();
    mPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    long mEnd = System.nanoTime();
    long mMillis = (mEnd - mStart) / 1_000_000;
    return mMillis;
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
