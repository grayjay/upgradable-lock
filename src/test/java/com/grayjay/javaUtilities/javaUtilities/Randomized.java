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
              case 0: case 1: read(); break;
              case 2: upgradable(); break;
              case 3: write(); break;
              default: throw new AssertionError();
            }
          }
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    };
  }

  private void read() throws InterruptedException {
    myLock.lock(Mode.READ);
    try {
      int mStart = myCount;
      maybeSleep();
      int mEnd = myCount;
      checkStartAndEnd(mStart, mEnd);
    } finally {
      myLock.unlock();
    }
  }

  private void upgradable() throws InterruptedException {
    myLock.lock(Mode.UPGRADABLE);
    try {
      int mStart = myCount;
      maybeSleep();
      int mNext;
      if (mStart % 2 == 1) {
        mNext = mStart + 1;
        if (nextInt(2) == 0) {
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

  private void write() throws InterruptedException {
    myLock.lock(Mode.WRITE);
    try {
      int mStart = myCount;
      int mNext = mStart + 1;
      maybeSleep();
      myCount = mNext;
      int mEnd = myCount;
      checkStartAndEnd(mNext, mEnd);
    } finally {
      myLock.unlock();
    }
  }

  private static void maybeSleep() throws InterruptedException {
    if (nextInt(50) == 0) {
      Thread.sleep(0, nextInt(3));
    } else if (nextInt(500) == 0) {
      Thread.sleep(nextInt(3));
    }
  }
  
  private static int nextInt(int aBound) {
    return ThreadLocalRandom.current().nextInt(aBound);
  }
  
  private static void checkStartAndEnd(int aStart, int aEnd) {
    if (aStart != aEnd) {
      throw new ConcurrentModificationException("count: " + aStart + ", " + aEnd);
    }
  }
}
