const redis = require('redis');

async function test(event) {// const redisClient = redis.createClient(6379, 'clustercfg.specter-app-dev-redis.k8wqvq.memorydb.ap-south-1.amazonaws.com');
    const redisClient = redis.createClient({
        host : '127.0.0.1',
        port : 6379
    });
    try {
        const userId = event.userId; // Replace this with your actual logic to get the userId
        const status = event.status; // Replace this with your actual logic to get the status

        // Create a Redis client

        await redisClient.connect();

        await redisClient.set(userId, status);
        const result = await redisClient.get(userId);
        console.log('result', result);

        // Set the key-value pair
        // await new Promise((resolve, reject) => {
        //     redisClient.set(userId, status, function (err, reply)  {
        //         if (err) {
        //             reject(err);
        //         } else {
        //             console.log('Key-Value set/update successful');
        //             resolve(reply);
        //         }
        //     });
        // });

        // Return a response
        return {
            statusCode: 200,
            body: JSON.stringify({message: 'Key-Value set/update successful'})
        };
    } catch (err) {
        console.log('Error', err);
    }
    finally {
        // Close the Redis connection
        redisClient.quit().then(() => {
            console.log('Connection is closed');
        }).catch(err => {
            console.log('Error closing connection', err);
        });
    }
}

test({
    userId: "18e1b5bc-ccb7-4668-b94a-ec80a19ac627",
    status: "processing"
});
