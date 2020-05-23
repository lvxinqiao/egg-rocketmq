'use strict';
const BaseType = require('./baseType');
class Normal extends BaseType {
  constructor(options) {
    super(options);
    this.normalTopics = this.topics.filter(v => v.type === 'normal');
    this.normalProducer = {};
    this.normalConsumer = {};
    this.init(this.client);
  }

  init() {
    this.normalTopics.forEach(v => {
      const normalProducer = this.client.getProducer(
        this.instance_id,
        v.name,
        this.group_default || ''
      );
      this.normalProducer.default = normalProducer;
      const normalConsumer = this.client.getConsumer(
        this.instance_id,
        v.name,
        this.group_default || ''
      );
      this.normalConsumer.default = normalConsumer;
    });
  }

  getProducer() {
    if (this.normalProducer.default) return this.normalProducer.default;
    return null;
  }

  getConsumer() {
    if (this.normalConsumer.default) return this.normalConsumer.default;
    return null;
  }

  /**
   * 消费消息
   * @param msgCount
   * @param waitSeconds
   * @return {Promise.<*>}
   */
  async consumeMessage(msgCount, waitSeconds) {
    const res = await this.normalConsumer.default.consumeMessage(msgCount, waitSeconds);
    return res;
  }

  /**
   * 移除消息
   * @param message
   * @return {Promise.<*>}
   */
  async handlerReceipt(message = {}) {
    if (!message.ReceiptHandle) return false;
    const res = await this.normalConsumer.default.ackMessage([ message.ReceiptHandle ]);
    return res;
  }
}

module.exports = Normal;
