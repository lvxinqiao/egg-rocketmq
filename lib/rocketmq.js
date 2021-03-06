'use strict';
const { MessageProperties } = require('@aliyunmq/mq-http-sdk');
const delay = require('./Delay');
const normal = require('./Normal');
const camelCase = require('camelcase');
const trans = require('./Transaction');
class RocketMQ {
  constructor(app, options) {
    this.options = options;
    this.app = app;
    this.type = {
      delay: new delay(options),
      normal: new normal(options),
      trans: new trans(options),
    };
  }

  /**
   * 执行方法
   * @param key
   * @param message
   * @return {Promise.<*>}
   */
  async funcService(message) {
    const { MessageTag, MessageBody } = message;
    const ctx = this.app.createAnonymousContext();
    let service = ctx.service;
    const layer = MessageTag.split('.').map(v => camelCase(v));
    try {
      const body = JSON.parse(MessageBody);
      for (const item of layer) {
        service = service[item];
      }
      service = service.bind({
        ctx,
        app: this.app,
        service: ctx.service,
      });
      if (service.constructor && service.constructor.name === 'AsyncFunction') {
        const res = await service(body);
        return res;
      } else if (
        service.constructor &&
        service.constructor.name === 'Function'
      ) {
        const res = service(body);
        return res;
      }
      return true;

    } catch (e) {
      this.app.logger.error('Func Service: %s', e);
      return false;
    }
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
    if (messageKey) messageProperties.messageKey(messageKey);
    if (Object().toString.call(msgProps) === '[object Object]') {
      for (const key in msgProps) {
        if (msgProps.hasOwnProperty(key)) {
          const value = msgProps[key];
          messageProperties.putProperty(key, value);
        }
      }
    }
    try {
      this.type[topic].getRule(messageProperties, time);
      const res = await producer.publishMessage(JSON.stringify(body), tag || '', messageProperties);
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
            await this.funcService(message);
          } catch (e) {
            this.app.logger.error('[ROCKETMQ_SUBSCRIPTION_MESSAGE] Consume Service Error: TAG:%s, ERROR:%s', message.MessageTag, e);
          }
          const res = await this.type[mode].handlerReceipt(message);
          if (res.code !== 204) {
            this.app.logger.warn('[ROCKETMQ_SUBSCRIPTION_MESSAGE] Message Fail: %s, MODE: %s ', res, mode);
          } else {
            this.app.logger.info('[ROCKETMQ_SUBSCRIPTION_MESSAGE] Message Success: %s', res, mode);
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
