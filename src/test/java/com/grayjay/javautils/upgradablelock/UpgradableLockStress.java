package com.grayjay.javautils.upgradablelock;

import java.util.*;
import java.util.concurrent.*;

import com.grayjay.javautils.upgradablelock.UpgradableLock.Mode;


public class UpgradableLockStress {
  private static final int N_THREADS =
      Runtime.getRuntime().availableProcessors() + 1;
  private static final int N_STEPS = 1_000_000;
  
  private final UpgradableLock myLock = new UpgradableLock();
  private int myCount = 0;
  private final List<Thread> myThreads = new CopyOnWriteArrayList<>();
  
  public static void main(String[] aArgs) throws InterruptedException {
    new UpgradableLockStress().test();
  }

  private void test() throws InterruptedException {
    long mStart = System.nanoTime();
    for (int i = 0; i < N_THREADS; i++) {
      Thread mThread = new Thread(newTask());
      myThreads.add(mThread);
    }
    for (Thread mThread : myThreads) {
      mThread.start();
    }
    for (Thread mThread : myThreads) {
      mThread.join();
    }
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
            double mChoice = random().nextDouble();
            if (mChoice < 0.02) interrupt();
            else if (mChoice < 0.5) read();
            else if (mChoice < 0.75) upgradable();
            else write();
          }
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    };
  }

  private void interrupt() {
    int mIndex = random().nextInt(N_THREADS);
    myThreads.get(mIndex).interrupt();
  }

  private void read() {
    if (tryLock(Mode.READ)) {
      try {
        int mStart = myCount;
        maybeSleep();
        int mEnd = myCount;
        checkStartAndEnd(mStart, mEnd);
      } finally {
        myLock.unlock();
      }
    }
  }

  private void upgradable() {
    if (tryLock(Mode.UPGRADABLE)) {
      try {
        int mStart = myCount;
        maybeSleep();
        int mNext = mStart;
        if (mStart % 2 == 0) {
          if (random().nextBoolean()) {
            if (tryUpgrade()) {
              mNext = mStart + 1;
              myCount = mNext;
              if (random().nextBoolean()) {
                myLock.downgrade();
              }
            }
          } else {
            if (tryLock(Mode.WRITE)) {
              mNext = mStart + 1;
              myCount = mNext;
              myLock.unlock();
            }
          }
        }
        int mEnd = myCount;
        checkStartAndEnd(mNext, mEnd);
      } finally {
        myLock.unlock();
      }
    }
  }

  private void write() {
    if (tryLock(Mode.WRITE)) {
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
  }
  
  private boolean tryLock(Mode aMode) {
    switch (random().nextInt(4)) {
      case 0: myLock.lock(aMode); return true;
      case 1: return myLock.tryLock(aMode);
      case 2:
        try {
          myLock.lockInterruptibly(aMode);
          return true;
        } catch (InterruptedException e) {
          return false;
        }
      case 3:
        try {
          TimeUnit mUnit = random().nextBoolean() ?
              TimeUnit.NANOSECONDS :
              TimeUnit.MICROSECONDS;
          long mTime = random().nextLong(-100, 100);
          return myLock.tryLock(aMode, mTime, mUnit);
        } catch (InterruptedException e) {
          return false;
        }
      default: throw new AssertionError();
    }
  }

  private boolean tryUpgrade() {
    switch (random().nextInt(4)) {
      case 0: myLock.upgrade(); return true;
      case 1: return myLock.tryUpgrade();
      case 2:
        try {
          myLock.upgradeInterruptibly();
          return true;
        } catch (InterruptedException e) {
          return false;
        }
      case 3:
        try {
          TimeUnit mUnit = random().nextBoolean() ?
              TimeUnit.NANOSECONDS :
              TimeUnit.MICROSECONDS;
          long mTime = random().nextLong(-100, 100);
          return myLock.tryUpgrade(mTime, mUnit);
        } catch (InterruptedException e) {
          return false;
        }
      default: throw new AssertionError();
    }
  }

  private static void maybeSleep() {
    ThreadLocalRandom mRandom = random();
    try {
      if (mRandom.nextInt(50) == 0) {
        Thread.sleep(0, mRandom.nextInt(3));
      } else if (mRandom.nextInt(500) == 0) {
        Thread.sleep(mRandom.nextInt(3));
      }
    } catch (InterruptedException e) {
      // return
    }
  }
  
  private static ThreadLocalRandom random() {
    return ThreadLocalRandom.current();
  }
  
  private static void checkStartAndEnd(int aStart, int aEnd) {
    if (aStart != aEnd) {
      throw new ConcurrentModificationException("count: " + aStart + ", " + aEnd);
    }
  }
}
