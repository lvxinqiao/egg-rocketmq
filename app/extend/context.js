'use strict';

module.exports = {
  /**
   * rocketmq Singleton instance
   * @member Context#rocketmq
   * @since 1.0.0
   * @see App#rocketmq
   */
  get rocketmq() {
    return this.app.rocketmq;
  },
};
