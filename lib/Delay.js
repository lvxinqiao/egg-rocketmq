'use strict';
const BaseType = require('./baseType');

class Delay extends BaseType {
  constructor(options) {
    super(options);
    this.delayTopics = this.topics.filter(v => v.type === 'delay');
    this.delayProducer = {};
    this.delayConsumer = {};
    this.init();
  }

  init() {
    this.delayTopics.forEach(v => {
      const delayProducer = this.client.getProducer(
        this.instance_id,
        v.name,
        this.group_default || ''
      );
      this.delayProducer.default = delayProducer;
      const delayConsumer = this.client.getConsumer(
        this.instance_id,
        v.name,
        this.group_default || ''
      );
      this.delayConsumer.default = delayConsumer;
    });
  }

  getRule(time, msgProps) {
    if (time && time <= Date.now()) {
      return false;
    }
    msgProps.startDeliverTime(time);
  }

  getProducer() {
    if (this.delayProducer.default) return this.normalProducer.default;
    return null;
  }

  getConsumer() {
    if (this.delayConsumer.default) return this.delayConsumer.default;
    return null;
  }

  /**
   * 消费消息
   * @param msgCount
   * @param waitSeconds
   * @return {Promise.<*>}
   */
  async consumeMessage(msgCount, waitSeconds) {
    const res = await this.delayConsumer.default.consumeMessage(msgCount, waitSeconds);
    return res;
  }

  /**
   * 移除消息
   * @param receiptHandles
   * @return {Promise.<*>}
   */
  async handlerReceipt(receiptHandles) {
    const res = await this.delayConsumer.default.ackMessage(receiptHandles);
    return res;
  }
}

module.exports = Delay;
