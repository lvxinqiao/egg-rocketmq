'use strict';
const {
  MQClient,
} = require('@aliyunmq/mq-http-sdk');
class BaseType {
  constructor(options) {
    this.options = options;
    this.instance_id = options.instance_id || '';
    this.topics = options.topics || [];
    this.group_default = options.group_default || 'GID_default';
    this.client = new MQClient(options.endpoint, options.accessKeyId, options.accessKeySecret);
  }

  getRule() {}
  consumeMessage() {}
}

module.exports = BaseType;

