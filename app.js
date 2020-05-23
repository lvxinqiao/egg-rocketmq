'use strict';
const RocketMQ = require('./lib/rocketmq');
module.exports = app => {
  if (app.config.rocketmq && !app.rocketmq) {
    app.rocketmq = new RocketMQ(app, app.config.rocketmq);
  }

  app.beforeStart(async () => {
    if (!app.rocketmq) {
      return;
    }
    const evns = app.config.rocketmq.evns || [];
    const isStart = evns.includes(app.config.Env);
    if (isStart) {
      app.rocketmq.startConsumer();
    }
    app.coreLogger.info('[ROCKETMQ]', 'started!');
  });
};
