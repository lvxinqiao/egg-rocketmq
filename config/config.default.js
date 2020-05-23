'use strict';

/**
 * egg-ali-rocketmq default config
 * @member Config#aliRocketmq
 * @property {String} SOME_KEY - some description
 */
exports.rocketmq = {
  accessKeyId: '',
  accessKeySecret: '',
  endpoint: '', // http 公网接入
  instance_id: '',
  group_default: '',
  num_of_messages: 1,
  wait_seconds: 3,
  evns: [ 'mq_dev' ],
  topics: [
    {
      name: 'order',
      type: 'normal',
      enable: true,
    },
    {
      name: 'delay',
      type: 'delay',
      enable: true,
    },
    {
      name: 'tran',
      type: 'trans',
      enable: true,
    },
  ],
};
