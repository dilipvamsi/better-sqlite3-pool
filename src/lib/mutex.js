/**
 * @file lib/mutex.js
 * @description A simple Mutual Exclusion lock (Mutex) implementation with a FIFO queue.
 *
 * This class is used to serialize access to the single Writer worker.
 * Unlike standard JavaScript concurrency (which relies on the Event Loop),
 * database write operations require strict serialization to ensure ACID compliance
 * and prevent `SQLITE_BUSY` errors at the file system level.
 *
 * ARCHITECTURE NOTE:
 * This Mutex does not handle timeouts or transaction lifecycles.
 * That logic is handled by the `Connection` class (see `lib/connection.js`),
 * which owns this lock and ensures it is released even if the user code crashes.
 */

class Mutex {
  constructor() {
    /**
     * @type {Array<Function>}
     * A queue of `resolve` functions for Promises waiting to acquire the lock.
     * Processed in First-In-First-Out (FIFO) order.
     */
    this._queue = [];

    /**
     * @type {boolean}
     * Indicates whether the mutex is currently held by a consumer.
     */
    this._locked = false;
  }

  /**
   * Checks if the mutex is currently locked.
   * Useful for debugging or leak detection logic.
   * @returns {boolean} True if locked.
   */
  isLocked() {
    return this._locked;
  }

  /**
   * Acquires the lock.
   * - If the lock is free, marks it as locked and returns immediately.
   * - If the lock is held, returns a Promise that resolves only when the lock is released.
   *
   * @returns {Promise<void>} Resolves when the lock has been successfully acquired.
   */
  acquire() {
    if (this._locked) {
      // If locked, we pause execution by returning a new Promise.
      // We push the 'resolve' function into the queue.
      // This promise will only resolve when release() calls it.
      return new Promise((resolve) => this._queue.push(resolve));
    }

    // If free, take the lock immediately.
    this._locked = true;
    return Promise.resolve();
  }

  /**
   * Releases the lock.
   * - If there are waiters in the queue, effectively "passes the baton" to the next waiter
   *   without unlocking (maintaining the lock state).
   * - If the queue is empty, sets the state to unlocked.
   */
  release() {
    if (this._queue.length > 0) {
      // Dequeue the next waiting task.
      const nextResolve = this._queue.shift();

      // Trigger the next task.
      // NOTE: We do NOT set this._locked = false here.
      // The lock is implicitly transferred to the next task in the queue.
      if (nextResolve) {
        nextResolve();
      }
    } else {
      // No one is waiting. The resource is truly free.
      this._locked = false;
    }
  }
}

module.exports = Mutex;
