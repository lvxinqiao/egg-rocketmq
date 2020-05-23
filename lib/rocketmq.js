'use strict';
const { MessageProperties } = require('@aliyunmq/mq-http-sdk');
const delay = require('./Delay');
const normal = require('./Normal');
const trans = require('./Transaction');
class RocketMQ {
  constructor(options, app) {
    this.options = options;
    this.app = app;
    this.map = new Map();
    this.type = {
      delay: new delay(options),
      normal: new normal(options),
      trans: new trans(options),
    };
  }

  registerFunc(services = {}) {
    for (const key in services) {
      if (this.map.has(key)) throw new Error('exist repeat key');
      if (typeof services[key] !== 'function') throw new Error(`key为${key}的值不是一个函数`);
      this.map.set(key, services[key]);
    }
  }

  /**
   * 执行方法
   * @param key
   * @return {Promise.<*>}
   */
  async funcService(key) {
    const result = this.map.get(key);
    if (result && typeof result === 'function') {
      const res = await result();
      return res;
    }
    return null;
  }

  /**
   * 生产消息
   * @param topic
   * @param tag
   * @param body
   * @param messageKey
   * @param msgProps
   * @param time
   * @return {Promise.<*>}
   */
  async publishMessage(
    topic,
    tag,
    body,
    { messageKey, msgProps, time } = {}
  ) {
    if (!this.type[topic]) throw new Error('话题类型错误，此类型只包括trans、delay、normal');
    const producer = this.type[topic].getProducer();
    if (!producer) { throw new Error('rocketmq生产者没有创建'); }
    const messageProperties = new MessageProperties();
    if (messageKey) msgProps.messageKey(messageKey);
    if (Object().toString.call(msgProps) === '[object Object]') {
      for (const key in msgProps) {
        if (msgProps.hasOwnProperty(key)) {
          const value = msgProps[key];
          messageProperties.putProperty(key, value);
        }
      }
    }
    try {
      this.type[topic].getRule(time);
      const res = await producer.publishMessage(body, tag || '', msgProps);
      return res;
    } catch (error) {
      this.app.logger.error('[ROCKETMQ_PUBLISH_MESSAGE] error: %s', error);
      return null;
    }
  }

  /**
   * 消费消息
   * @param consumer
   * @param numOfMessages
   * @param waitSeconds
   * @param mode
   * @return {Promise.<boolean>}
   */
  async subscriptionMessage(consumer, mode = 'normal', numOfMessages = 1, waitSeconds = 3) {
    if (!consumer) return false;
    if (!this.type[mode]) throw new Error('消费消息类型错误，此类型只包括trans、delay、normal');
    this.app.logger.info(`[ROCKETMQ_SUBSCRIPTION_MESSAGE] Start Customer: PID:${process.pid}, TOPIC: ${consumer.topic}, MODE: ${mode}`);
    while (true) {
      try {
        const res = await this.type[mode].consumeMessage(numOfMessages, waitSeconds);
        if (!res || res.code !== 200) {
          const err = new Error('ROCKETMQ_SUBSCRIPTION_MESSAGE] CONSUME_MESSAGE_ERROR');
          err.result = res;
          throw err;
        }
        const bodys = res.body || [];
        bodys.forEach(async message => {
          try {
            await this.funcService(message.MessageTag);
            console.log(message, mode, '===========');
            const res = await this.type[mode].handlerReceipt(message);
            if (res.code !== 204) {
              this.app.logger.warn('[ROCKETMQ_SUBSCRIPTION_MESSAGE] Message Fail: %s, MODE: %s ', res, mode);
            } else {
              this.app.logger.info('[ROCKETMQ_SUBSCRIPTION_MESSAGE] Message Success: %s', res, mode);
            }
          } catch (e) {
            this.app.logger.error('[ROCKETMQ_SUBSCRIPTION_MESSAGE] Consume Service Error: TAG:%s, ERROR:%s', message.MessageTag, e);
          }
        });
      } catch (e) {
        if (e.Code && e.Code.indexOf('MessageNotExist') > -1) {
          this.app.logger.debug('[ROCKETMQ_SUBSCRIPTION_MESSAGE] ERROR Customer:, RequestId:%s, Code:%s', e.RequestId, e.Code);
        } else {
          this.app.logger.error(`
        [ROCKETMQ_SUBSCRIPTION_MESSAGE]
         ERROR Customer:
         PID:${process.pid},
         TOPIC: ${consumer.topic},
          MODE: ${mode}, ERROR:${e}`
          );
        }
      }
    }
  }

  /**
   * 启动consumer 轮训
   * @return {Promise.<void>}
   */
  async startConsumer() {
    for (const key in this.type) {
      if (key === 'delay') continue;
      const consumer = this.type[key].getConsumer();
      await this.subscriptionMessage(consumer, key);
    }
  }
}

module.exports = RocketMQ;
