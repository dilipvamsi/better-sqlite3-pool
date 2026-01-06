/**
 * @file lib/mutex.js
 * @description A simple Promise-based Mutex for resource locking.
 */
class Mutex {
  constructor() {
    this.queue = [];
    this.locked = false;
  }

  /**
   * Acquires the lock. If locked, the caller waits in a queue.
   * @returns {Promise<void>} Resolves when the lock is acquired.
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
   * Releases the lock and grants it to the next waiter in the queue.
   */
  release() {
    if (this.queue.length > 0) {
      const nextUser = this.queue.shift();
      nextUser(); // Grant lock to next user (keep locked=true)
    } else {
      this.locked = false;
    }
  }
}

module.exports = Mutex;
