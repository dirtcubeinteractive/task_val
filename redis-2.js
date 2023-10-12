const redis = require('redis');

const client = redis.createClient({
    host: 'clustercfg.specter-app-dev-redis.k8wqvq.memorydb.ap-south-1.amazonaws.com',
    port: 6379 // Default Redis port
});

const key = 'your-key';
const value = 'your-value';


client.set(key, value, (err, reply) => {
    if (err) {
        console.error('Error:', err);
    } else {
        console.log('Key-value pair set successfully:', reply);
    }

    // Close the Redis connection
    client.quit();
});
