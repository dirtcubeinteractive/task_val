const axios = require('axios');
const Sequelize = require("sequelize");
const {MongoClient} = require("mongodb");

const init = async () => {
    try {
        const sequelize = new Sequelize(
            'gamehub_dev',
            'gamehub_admin',
            'Dirtcube2019',
            {
                host: 'gamehubdev.cx8tjkw161jy.ap-south-1.rds.amazonaws.com',
                dialect: 'postgres',
                port: 5432,
                logging: false,
            }
        );

        // Set up Mongoose connection
        const mongoURI = 'mongodb+srv://yashh:Yashkadam1234@gamestarz-dev.2tsh39e.mongodb.net/gamehub_dev?retryWrites=true&w=majority'; // Replace placeholders
        const client = new MongoClient(mongoURI, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });

        await client.connect();
        const db = client.db();

        // Delete all user data
        await sequelize.query(`delete from users where project_id='97083cc6-cd6c-4104-b71d-78a0c582d2a1';`);

        const aiti = db.collection('additemtoinventories');
        const me = db.collection('matchends');
        const uupm = db.collection('userupdateprogressionmarkers');
        const upw = db.collection('userupdatewallets');

        await aiti.deleteMany({});
        await me.deleteMany({});
        await uupm.deleteMany({});
        await upw.deleteMany({});

        const newUserResponse =  await axios.post('http://localhost:3000/v1/client/auth/signup-email', {
            email: "john@gmail.com",
            password: "123",
            projectId: "97083cc6-cd6c-4104-b71d-78a0c582d2a1",
            customParams : {
                "dummy" : 1
            }
        });

        const accessToken = newUserResponse.data.data.accessToken;
        const userId = newUserResponse.data.data.id;
        await firstMatch(userId, accessToken);
        await secondMatch(userId, accessToken);
        await thirdMatch(userId, accessToken);
        await fourthMatch(userId, accessToken);
        await fifthMatch(userId, accessToken);
    } catch (err) {
        console.log('err', err);
    }
}


async function firstMatch(userId, accessToken) {
    const firstMatchStartResponse = await axios.post('http://localhost:3000/v1/client/match-session/start', {
        matchId: "e51d3685-e18a-401a-ad8d-936d66c6ee24",
        userInfo: [
            {
                id: `${userId}`
            }
        ]
    }, {
        headers: {
            'Authorization': `Bearer ${accessToken}`
        }
    });

    const firstMatchEndResponse = await axios.post('http://localhost:3000/v1/client/match-session/end', {
        matchSessionId: `${firstMatchStartResponse.data.data.matchSessionId}`,
        userInfo: [
            {
                id: `${userId}`,
                outcome: 2,
                customParam: {
                    goals: 2,
                    premierleague: "false",
                    count: 1
                }
            }
        ]
    }, {
        headers: {
            'Authorization': `Bearer ${accessToken}`
        }
    });

    if (firstMatchEndResponse.status === 201 || firstMatchEndResponse.status === 200) {
        console.log('First match end successful');
    }
}

async function secondMatch(userId, accessToken) {
    const secondMatchStartResponse = await axios.post('http://localhost:3000/v1/client/match-session/start', {
        matchId: "e51d3685-e18a-401a-ad8d-936d66c6ee24",
        userInfo: [
            {
                id: `${userId}`
            }
        ]
    }, {
        headers: {
            'Authorization': `Bearer ${accessToken}`
        }
    });

    const secondMatchEndResponse = await axios.post('http://localhost:3000/v1/client/match-session/end', {
        matchSessionId: `${secondMatchStartResponse.data.data.matchSessionId}`,
        userInfo: [
            {
                id: `${userId}`,
                outcome: 0,
                customParam: {
                    goals: 0,
                    premierleague: "false",
                    count: 1
                }
            }
        ]
    }, {
        headers: {
            'Authorization': `Bearer ${accessToken}`
        }
    });

    if (secondMatchEndResponse.status === 201 || secondMatchEndResponse.status === 200) {
        console.log('Second match end successful');
    }
}

async function thirdMatch(userId, accessToken) {
    const thirdMatchStartResponse = await axios.post('http://localhost:3000/v1/client/match-session/start', {
        matchId: "e51d3685-e18a-401a-ad8d-936d66c6ee24",
        userInfo: [
            {
                id: `${userId}`
            }
        ]
    }, {
        headers: {
            'Authorization': `Bearer ${accessToken}`
        }
    });

    const thirdMatchEndResponse = await axios.post('http://localhost:3000/v1/client/match-session/end', {
        matchSessionId: `${thirdMatchStartResponse.data.data.matchSessionId}`,
        userInfo: [
            {
                id: `${userId}`,
                outcome: 3,
                customParam: {
                    goals: 3,
                    premierleague: "true",
                    count: 1
                }
            }
        ]
    }, {
        headers: {
            'Authorization': `Bearer ${accessToken}`
        }
    });

    if (thirdMatchEndResponse.status === 201 || thirdMatchEndResponse.status === 200) {
        console.log('Third match end successful');
    }
}

async function fourthMatch(userId, accessToken) {
    const matchStartResponse = await axios.post('http://localhost:3000/v1/client/match-session/start', {
        matchId: "e51d3685-e18a-401a-ad8d-936d66c6ee24",
        userInfo: [
            {
                id: `${userId}`
            }
        ]
    }, {
        headers: {
            'Authorization': `Bearer ${accessToken}`
        }
    });

    const matchEndResponse = await axios.post('http://localhost:3000/v1/client/match-session/end', {
        matchSessionId: `${matchStartResponse.data.data.matchSessionId}`,
        userInfo: [
            {
                id: `${userId}`,
                outcome: 2,
                customParam: {
                    goals: 2,
                    premierleague: "true",
                    count: 1
                }
            }
        ]
    }, {
        headers: {
            'Authorization': `Bearer ${accessToken}`
        }
    });

    if (matchEndResponse.status === 201 || matchEndResponse.status === 200) {
        console.log('Fourth match end successful');
    }
}

async function fifthMatch(userId, accessToken) {
    const matchStartResponse = await axios.post('http://localhost:3000/v1/client/match-session/start', {
        matchId: "e51d3685-e18a-401a-ad8d-936d66c6ee24",
        userInfo: [
            {
                id: `${userId}`
            }
        ]
    }, {
        headers: {
            'Authorization': `Bearer ${accessToken}`
        }
    });

    const matchEndResponse = await axios.post('http://localhost:3000/v1/client/match-session/end', {
        matchSessionId: `${matchStartResponse.data.data.matchSessionId}`,
        userInfo: [
            {
                id: `${userId}`,
                outcome: 4,
                customParam: {
                    goals: 4,
                    premierleague: "true",
                    count: 1
                }
            }
        ]
    }, {
        headers: {
            'Authorization': `Bearer ${accessToken}`
        }
    });

    if (matchEndResponse.status === 201 || matchEndResponse.status === 200) {
        console.log('Fifth match end successful');
    }
}

init().then();