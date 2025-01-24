import IORedis from 'ioredis';

export const redisClient = new IORedis({
    host:'caching-17f435c1-jaiswalk008-project1.h.aivencloud.com',
    port:24056,
    username:"default",
    password:"AVNS_HVGyea8oU5rrDI2oO16"
})
export const redisPublisher = new IORedis({
    host:'caching-17f435c1-jaiswalk008-project1.h.aivencloud.com',
    port:24056,
    username:"default",
    password:"AVNS_HVGyea8oU5rrDI2oO16"
})
export const redisSubscriber = new IORedis({
    host:'caching-17f435c1-jaiswalk008-project1.h.aivencloud.com',
    port:24056,
    username:"default",
    password:"AVNS_HVGyea8oU5rrDI2oO16"
})

// redisClient.subscribe('columnMappingTaskStatus', (err, count) => {
//     if (err) {
//       console.error('Error subscribing to taskStatus channel:', err);
//       return;
//     }
//     console.log(`Subscribed to ${count} channel(s). Listening for updates...`);
//   });
  
//   // Listen for messages on the `taskStatus` channel
//   redisClient.on('message', (channel, message) => {
//     console.log(`Received message on channel ${channel}:`, message);
//     // handleTaskUpdate(message);
//   });
  