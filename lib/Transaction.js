'use strict';
const BaseType = require('./baseType');

class Transaction extends BaseType {
  constructor(options) {
    super(options);
    this.transTopics = this.topics.filter(v => v.type === 'trans');
    this.transProducer = {};
    this.transCustomer = {};
    this.init();
  }

  init() {
    this.transTopics.forEach(v => {
      const trans = this.client.getTransProducer(
        this.instance_id,
        v.name,
        this.group_default || ''
      );
      this.transProducer.default = trans;
      this.transCustomer.default = trans;
    });
  }

  getProducer() {
    if (this.transProducer.default) return this.transProducer.default;
    return null;
  }

  getConsumer() {
    if (this.transCustomer.default) return this.transCustomer.default;
    return null;
  }

  getRule(msgProps, transCheckTime) {
    transCheckTime = transCheckTime || 10;
    msgProps.transCheckImmunityTime(transCheckTime);
  }

  /**
   * 提交半消息
   * @param msgCount
   * @param waitSeconds
   * @return {Promise.<*>}
   */
  async consumeMessage(msgCount = 1, waitSeconds = 3) {
    const res = await this.transCustomer.default.consumeHalfMessage(msgCount, waitSeconds);
    return res;
  }

  /**
   * 提交事务消息
   * @param message
   * @return {Promise.<*>}
   */
  async handlerReceipt(message = {}) {
    const res = await this.transCustomer.default.commit(message.ReceiptHandle);
    return res;
  }

  /**
   * 回滚事务消息
   * @param ReceiptHandle
   * @return {Promise.<*>}
   */
  async rollback(ReceiptHandle) {
    const res = await this.transCustomer.default.rollback(ReceiptHandle);
    return res;
  }
}

module.exports = Transaction;
