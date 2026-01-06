/**
 * @file lib/mutex.js
 * @description A simple Promise-based Mutex (Mutual Exclusion) for synchronizing async operations.
 * Used primarily to serialize write operations on the Writer worker.
 */

/**
 * @class Mutex
 * @description Ensures that only one asynchronous task can access a shared resource at a time.
 * It uses a FIFO (First-In-First-Out) queue to manage waiting tasks.
 */
class Mutex {
  /**
   * Create a new Mutex instance.
   */
  constructor() {
    /** * Queue of `resolve` functions for Promises waiting for the lock.
     * @type {Array<Function>}
     * @private
     */
    this.queue = [];

    /** * Indicates if the mutex is currently held by a task.
     * @type {boolean}
     * @private
     */
    this.locked = false;
  }

  /**
   * Acquires the lock.
   * If the mutex is unlocked, it locks immediately and resolves.
   * If the mutex is locked, the caller is added to the queue and waits.
   * @returns {Promise<void>} Resolves when the lock is successfully acquired.
   */
  acquire() {
    return new Promise((resolve) => {
      if (this.locked) {
        this.queue.push(resolve);
      } else {
        this.locked = true;
        resolve();
      }
    });
  }

  /**
   * Releases the lock.
   * If there are waiting tasks, the lock is passed directly to the next task in line (FIFO).
   * If the queue is empty, the mutex is set to unlocked.
   * @returns {void}
   */
  release() {
    if (this.queue.length > 0) {
      const nextUser = this.queue.shift();
      // Pass the lock ownership directly to the next user without unlocking
      if (nextUser) nextUser();
    } else {
      this.locked = false;
    }
  }
}

module.exports = Mutex;
