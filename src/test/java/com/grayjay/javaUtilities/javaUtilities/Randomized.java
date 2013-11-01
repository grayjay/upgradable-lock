package javaUtilities;

import java.util.ConcurrentModificationException;
import java.util.concurrent.*;

import javaUtilities.UpgradableLock.Mode;


public class Randomized {
  private static final int N_THREADS =
      Runtime.getRuntime().availableProcessors() + 1;
  private static final int N_STEPS = 10_000;
  
  private final UpgradableLock myLock = new UpgradableLock();
  private int myCount = 0;
  
  public static void main(String[] aArgs) throws InterruptedException {
    new Randomized().test();
  }

  private void test() throws InterruptedException {
    long mStart = System.nanoTime();
    ExecutorService mPool = Executors.newFixedThreadPool(N_THREADS);
    for (int i = 0; i < N_THREADS; i++) {
      mPool.execute(newTask());
    }
    mPool.shutdown();
    mPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    long mTotalMicros = (System.nanoTime() - mStart) / 1_000;
    if (!myLock.tryLock(Mode.WRITE)) {
      throw new AssertionError("Lock is still locked: " + myLock);
    }
    System.out.println("final count = " + myCount);
    System.out.println("total time = " + mTotalMicros / 1_000 + "ms");
    long mMicrosPerStep = Math.round(((double) mTotalMicros) / N_STEPS);
    System.out.println(mMicrosPerStep + "\u00B5s/step");
  }
  
  private Runnable newTask() {
    return new Runnable() {
      @Override
      public void run() {
        try {
          int mSteps = N_STEPS/N_THREADS;
          for (int i = 0; i < mSteps; i++) {
            switch (i % 4) {
              case 0: case 1: read(i); break;
              case 2: upgradable(i); break;
              case 3: write(i); break;
              default: throw new AssertionError();
            }
          }
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }

      private void read(int i) throws InterruptedException {
        myLock.lock(Mode.READ);
        try {
          int mStart = myCount;
          maybeSleep(i);
          int mEnd = myCount;
          checkStartAndEnd(mStart, mEnd);
        } finally {
          myLock.unlock();
        }
      }

      private void upgradable(int i) throws InterruptedException {
        myLock.lock(Mode.UPGRADABLE);
        try {
          int mStart = myCount;
          maybeSleep(i);
          int mNext;
          if (mStart % 2 == 1) {
            mNext = mStart + 1;
            if (i % 5 < 3) {
              myLock.upgrade();
              myCount = mNext;
            } else {
              myLock.lock(Mode.WRITE);
              try {
                myCount = mNext;
              } finally {
                myLock.unlock();
              }
            }
          } else mNext = mStart;
          int mEnd = myCount;
          checkStartAndEnd(mNext, mEnd);
        } finally {
          myLock.unlock();
        }
      }

      private void write(int i) throws InterruptedException {
        myLock.lock(Mode.WRITE);
        try {
          int mStart = myCount;
          int mNext = mStart + 1;
          maybeSleep(i);
          myCount = mNext;
          int mEnd = myCount;
          checkStartAndEnd(mNext, mEnd);
        } finally {
          myLock.unlock();
        }
      }

      private void maybeSleep(int i) throws InterruptedException {
        if (i % 53 == 0) {
          Thread.sleep(0, i % 3);
        } else if (i % 503 == 0) {
          Thread.sleep(i % 3);
        }
      }
    };
  }
  
  private static void checkStartAndEnd(int aStart, int aEnd) {
    if (aStart != aEnd) {
      throw new ConcurrentModificationException("count: " + aStart + ", " + aEnd);
    }
  }
}
