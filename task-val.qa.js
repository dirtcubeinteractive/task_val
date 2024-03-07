const express = require('express');
const app = express();
const bodyParser = require('body-parser');

app.use(bodyParser.json());
app.use(bodyParser.urlencoded());

const Sequelize = require('sequelize');
const {MongoClient} = require('mongodb');
const {v4: uuidv4} = require('uuid');
const {QueryTypes} = require("sequelize");
const {Engine} = require('json-rules-engine');
const axios = require('axios');

app.listen(4000, () => {
    console.log('Server has started on PORT 4000');
});

app.post('/test-run', async (req, res) => {
    const {eventId, projectId, parameterIds, userId, paramDetails, levelSystemDetails, collectionName} = req.body; // You should replace this with the actual way to extract these values from the event

    const sequelize = new Sequelize(
        'dirtcube-specterapp-quality-assurance',
        'admin',
        'Dirtcube2019',
        {
            host: '3.108.113.179',
            dialect: 'postgres',
            port: 5432,
            logging: false,
        }
    );

    // Set up Mongoose connection
    const mongoURI = 'mongodb://admin:Dirtcube2019@3.108.113.179:27017/dirtcube-specterapp-quality-assurance?retryWrites=true&w=majority'; // Replace placeholders
    const client = new MongoClient(mongoURI, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
    });


    try {
        console.log('body', req.body);
        await client.connect();
        const db = client.db();

        const taskParametersCollection = db.collection('taskparameters'); // Use your collection name

        const levelMatchArray = levelSystemDetails.length ? levelSystemDetails.map(detail => ({
            "levelSystemLevelDetails": {
                "$elemMatch": {
                    "levelSystemId": detail.levelSystemId,
                    "level": {$lte: detail.level}
                }
            }
        })) : [];

        const orQuery = [];

        if (levelMatchArray.length) {
            orQuery.push({$and: levelMatchArray},
                {levelSystemLevelDetails: {$size: 0}},
                {levelSystemLevelDetails: {$exists: false}})
        } else {
            orQuery.push({levelSystemLevelDetails: {$size: 0}},
                {levelSystemLevelDetails: {$exists: false}})
        }

        let tasks;
        if (parameterIds.length) {
            tasks = await taskParametersCollection.find({
                eventId: eventId, // Use the retrieved event's ID
                projectId: projectId, // Use the provided projectId
                $or: orQuery,
                // "parameters": {
                //     $elemMatch: {
                //         "parameterId": {
                //             $in: parameterIds
                //         }
                //     }
                // },
            }).toArray();
        }

        if (!parameterIds.length) {
            tasks = await taskParametersCollection.find({
                eventId: eventId, // Use the retrieved event's ID
                projectId: projectId, // Use the provided projectId
                $or: orQuery,
                "parameters": null,
            }).toArray();
        }

        console.log('task', tasks);

        for (let task of tasks) {
            try {
                console.log('taskId', task.taskId);
                const utsc = db.collection('usertaskstatus');
                let taskValidationInit = false;

                if (!task.parameters || !task.parameters.length) {
                    console.log('task at 102', task);
                    const dbTask = await sequelize.query(`select * from tasks where id=:taskId`, {
                        replacements: {
                            taskId: task.taskId
                        },
                        raw: true,
                        nest: true
                    });

                    console.log('dbTask at 111', dbTask);

                    const dbTaskBus = await sequelize.query(`select * from task_bus where task_id=:taskId and user_id=:userId`, {
                        replacements: {
                            taskId: task.taskId,
                            userId : userId
                        },
                        raw: true,
                        nest: true
                    })

                    console.log('dbTaskBus at 122', dbTaskBus);

                    if (!dbTaskBus.length) {
                        console.log('task is passed at 125');
                        const taskStatus = dbTask[0].reward_claim === 'automatic' ? 'reward_claimed' : 'completed';
                        await sequelize.query(`insert into task_bus(id, status, meta, project_id, user_id, task_id, task_group_id, active, archive, created_at, updated_at)
values (uuid_generate_v4(), '${taskStatus}', null, '${projectId}', '${userId}', '${task.taskId}', null, true, false, now(), now())`, {
                            type: QueryTypes.INSERT,
                            nest: true
                        });

                        await utsc.insertOne({
                            taskId: task.taskId,
                            projectId: projectId,
                            userId: userId,
                            status: 'succeed'
                        });

                        await axios.post('http://localhost:3000/v1/task/grantReward', {
                            userId: userId,
                            eventId: eventId,
                            taskId: task.taskId
                        });
                    }

                    if (task.taskGroupId) {
                        const noOfConfigTasks = await sequelize.query(`select id from tasks where task_group_id='${task.taskGroupId}';`, {
                            type: QueryTypes.SELECT,
                            nest: true,
                            raw: true
                        });

                        const ids = noOfConfigTasks.map(item => `'${item.id}'`).join(', ');
                        const noOfTasksCompleted = await sequelize.query(`select count(*) from task_bus where task_id in (${ids});`, {type: QueryTypes.SELECT});
                        if (noOfTasksCompleted[0].count >= noOfConfigTasks.length) {
                            const dbTaskGroup = await sequelize.query(`select * from task_groups where id=:taskGroupId`, {
                                replacements: {
                                    taskGroupId: task.taskGroupId
                                },
                                raw: true,
                                nest: true
                            });

                            const dbTaskGroupTaskBus = await sequelize.query(`select * from task_bus where task_group_id=:taskGroupId`, {
                                replacements: {
                                    taskGroupId: task.taskGroupId
                                },
                                raw: true,
                                nest: true
                            })

                            if (!dbTaskGroupTaskBus.length) {
                                const taskBusStatus = dbTaskGroup[0].reward_claim === 'automatic' ? 'reward_claimed' : 'completed';

                                // Task group is completed
                                await sequelize.query(`insert into task_bus(id, status, meta, project_id, user_id, task_id, task_group_id, active, archive, created_at, updated_at)
                     values (uuid_generate_v4(), '${taskBusStatus}', null, '${projectId}', '${userId}', null, '${task.taskGroupId}', true, false, now(), now())`, {
                                    type: QueryTypes.INSERT,
                                    nest: true
                                });

                                await axios.post('http://localhost:3000/v1/task/grantReward', {
                                    userId: userId,
                                    eventId: eventId,
                                    taskGroupId: task.taskGroupId
                                });
                            }
                        }
                    }
                }

                if (task.parameters && task.parameters.length) {
                    let shouldEvaluate = true

                    for (let param of task.parameters) {
                        // Task is one time
                        if (!task.isRecurring) {
                            console.log('task at 199', task.taskId);
                            const dbTaskBus = await sequelize.query(`select * from task_bus where task_id='${task.taskId}' and user_id='${userId}' limit 1;`, {
                                type: QueryTypes.SELECT,
                                nest: true,
                                raw: true
                            });

                            console.log('dbTaskBus at 206', dbTaskBus)

                            if (dbTaskBus.length) {
                                shouldEvaluate = false;
                            }

                            if ((!dbTaskBus || !dbTaskBus.length) && param.incrementalType === 'cumulative') {
                                taskValidationInit = true
                                const userUpdateWalletCollection = db.collection(collectionName);
                                console.log('param ', param);
                                const pipeline = getAggregateQuery({
                                    parameters: task.parameters,
                                    userId,
                                    limit: param.noOfRecords || null,
                                    startTime: param.startTime || null,
                                    endTime: param.endTime || null,
                                    businessLogic: task.businessLogic
                                });
                                const result = await userUpdateWalletCollection.aggregate(pipeline).toArray();

                                if (result.length) {
                                    paramDetails[param.parameterName] = result[0][param.parameterName + "Sum"]
                                }
                            }
                        }

                        // Task is everytime
                        if (task.isRecurring) {
                            // const dbDoc = await utsc.find({userId: userId, taskId: task.taskId}).toArray();
                            // if (dbDoc) {
                            //     console.log('Doc found');
                            // }

                            const userUpdateWalletCollection = db.collection(collectionName);

                            const dbDoc = await userUpdateWalletCollection.find({userId: userId}).toArray();

                            if (param.incrementalType === 'cumulative') {
                                taskValidationInit = true
                                if (param.noOfRecords) {
                                    const modResult = dbDoc.length ? dbDoc.length % param.noOfRecords : param.noOfRecords;
                                    param.noOfRecords = modResult ? modResult : param.noOfRecords
                                }

                                if (!param.noOfRecords && param.startTime) {
                                    const dbTaskBus = await sequelize.query(`select * from task_bus where task_id='${task.taskId}' and user_id='${userId}' order by created_at desc limit 1;`, {
                                        type: QueryTypes.SELECT,
                                        nest: true,
                                        raw: true
                                    });

                                    if (dbTaskBus && dbTaskBus.length) {
                                        param.startTime = dbTaskBus[0].created_at
                                    }
                                }

                                const userUpdateWalletCollection = db.collection(collectionName);
                                const pipeline = getAggregateQuery({
                                    parameters: task.parameters,
                                    userId,
                                    limit: param.noOfRecords || null,
                                    startTime: param.startTime || null,
                                    endTime: param.endTime || null,
                                    businessLogic: task.businessLogic
                                });
                                const result = pipeline ? await userUpdateWalletCollection.aggregate(pipeline).toArray() : [];
                                if (result.length) {
                                    paramDetails[param.parameterName] = result[0][param.parameterName + "Sum"]
                                }
                            }
                        }
                    }

                    let ruleEngine = new Engine();
                    ruleEngine.addRule({
                        conditions: task.businessLogic,
                        name: 'test',
                        event: '',
                        onFailure: async () => {
                            console.log('task failed')
                            if (taskValidationInit) {
                                await utsc.insertOne({
                                    taskId: task.taskId,
                                    projectId: projectId,
                                    userId: userId,
                                    status: 'failed'
                                })
                            }
                        },
                        onSuccess: async () => {
                            console.log('shouldEvaluate',shouldEvaluate);
                            if (shouldEvaluate) {
                                const dbTask = await sequelize.query(`select * from tasks where id=:taskId`, {
                                    replacements: {
                                        taskId: task.taskId
                                    },
                                    raw: true,
                                    nest: true
                                });

                                const dbTaskBus = await sequelize.query(`select * from task_bus where task_id=:taskId and user_id=:userId`, {
                                    replacements: {
                                        taskId: task.taskId,
                                        userId : userId
                                    },
                                    raw: true,
                                    nest: true
                                });

                                console.log('dbTaskBus', dbTaskBus);

                                if (!dbTaskBus.length || dbTask[0].is_recurring === true) {
                                    const taskStatus = dbTask[0].reward_claim === 'automatic' ? 'reward_claimed' : 'completed';
                                    const rewardMode = dbTask[0].reward_claim === 'automatic' ? 'server' : 'client';
                                    const rewardStatus = dbTask[0].reward_claim === 'automatic' ? 'completed' : 'pending';
                                    console.log('task passed');
                                    await sequelize.query(`insert into task_bus(id, status, meta, project_id, user_id, task_id, task_group_id, active, archive, created_at, updated_at)
values (uuid_generate_v4(), '${taskStatus}', null, '${projectId}', '${userId}', '${task.taskId}', null, true, false, now(), now())`, {
                                        type: QueryTypes.INSERT,
                                        nest: true
                                    });

                                    await utsc.insertOne({
                                        taskId: task.taskId,
                                        projectId: projectId,
                                        userId: userId,
                                        status: 'succeed'
                                    });
                                    console.log('Making an api call after task is passed')
                                    await axios.post('http://localhost:3000/v1/task/grantReward', {
                                        userId: userId,
                                        eventId: eventId,
                                        taskId: task.taskId
                                    });
                                }

                                if (task.taskGroupId) {
                                    console.log('Inside the task group call');
                                    const dbTaskGroupBus = await sequelize.query(`select * from task_bus where task_group_id='${task.taskGroupId}' and user_id='${userId}' limit 1;`, {
                                        type: QueryTypes.SELECT,
                                        nest: true,
                                        raw: true
                                    });

                                    if (!dbTaskGroupBus.length) {
                                        const noOfConfigTasks = await sequelize.query(`select id from tasks where task_group_id='${task.taskGroupId}' and user_id='${userId}';`, {
                                            type: QueryTypes.SELECT,
                                            nest: true,
                                            raw: true
                                        });

                                        const ids = noOfConfigTasks.map(item => `'${item.id}'`).join(', ');

                                        const noOfTasksCompleted = await sequelize.query(`select count(*) from task_bus where task_id in (${ids});`, {type: QueryTypes.SELECT});

                                        if (Number(noOfTasksCompleted[0].count) >= noOfConfigTasks.length) {
                                            const dbTaskGroup = await sequelize.query(`select * from task_groups where id=:taskGroupId`, {
                                                replacements: {
                                                    taskGroupId: task.taskGroupId
                                                },
                                                raw: true,
                                                nest: true
                                            });

                                            const dbTaskGroupTaskBus = await sequelize.query(`select * from task_bus where task_group_id=:taskGroupId`, {
                                                replacements: {
                                                    taskGroupId: task.taskGroupId
                                                },
                                                raw: true,
                                                nest: true
                                            })

                                            if (!dbTaskGroupTaskBus.length || dbTaskGroup[0].is_recurring === true) {
                                                const taskBusStatus = dbTaskGroup[0].reward_claim === 'automatic' ? 'reward_claimed' : 'completed';
                                                const taskBusRewardMode = dbTaskGroup[0].reward_claim === 'automatic' ? 'server' : 'client';
                                                const taskBusRewardStatus = dbTaskGroup[0].reward_claim === 'automatic' ? 'completed' : 'pending';

                                                // Task group is completed
                                                await sequelize.query(`insert into task_bus(id, status, meta, project_id, user_id, task_id, task_group_id, active, archive, created_at, updated_at)
                     values (uuid_generate_v4(), '${taskBusStatus}', null, '${projectId}', '${userId}', null, '${task.taskGroupId}', true, false, now(), now())`, {
                                                    type: QueryTypes.INSERT,
                                                    nest: true
                                                });

                                                console.log('Making an api call after task group evaluate');

                                                await axios.post('http://localhost:3000/v1/task/grantReward', {
                                                    userId: userId,
                                                    eventId: eventId,
                                                    taskGroupId: task.taskGroupId
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });
                    console.log('paramDetails after update', paramDetails);
                    await ruleEngine.run(paramDetails);
                    ruleEngine.stop();
                }
            } catch (err) {
                console.log('error', err);
            }
        }

        return res.json({success: true})
    } catch (err) {
        console.log('error', err);
        // return res.status(500).json({error: err});
    } finally {
        // Close Mongoose connection
        await client.close();

        // Close Sequelize connection
        await sequelize.close();
    }


    function getAggregateQuery({parameters, userId, limit, startTime, endTime, businessLogic}) {
        if (!limit && !startTime && !endTime) {
            function buildMatchExpression(condition) {
                let expressions = [];

                if (condition.all) {
                    expressions = expressions.concat(condition.all.map(clause => {
                        if (clause.all || clause.any) {
                            return buildMatchExpression(clause);
                        } else {
                            return clauseToMatch(clause);
                        }
                    }));
                }

                if (condition.any) {
                    expressions.push({
                        $or: condition.any.map(clause => {
                            if (clause.all || clause.any) {
                                return buildMatchExpression(clause);
                            } else {
                                return clauseToMatch(clause);
                            }
                        })
                    });
                }

                return expressions.length === 1 ? expressions[0] : {$and: expressions};
            }

            function clauseToMatch(clause) {
                const param = parameters.find(p => p.parameterName === clause.fact);
                let expression = {};

                if (param.incrementalType === "cumulative" && clause.operator === "greaterThanInclusive") {
                    expression = {fact: clause.fact, operator: clause.operator, value: clause.value};
                } else {
                    expression = {
                        $or: [
                            {[`data.defaultParams.${clause.fact}`]: clause.value},
                            {[`data.customParams.${clause.fact}`]: clause.value}
                        ]
                    };
                }
                return expression;
            }

            let matchConditions = buildMatchExpression(businessLogic);
            matchConditions = Array.isArray(matchConditions) ? matchConditions : [matchConditions];

            let initialMatchStage = {$match: {userId: userId}};

            let groupStage = {
                $group: {
                    _id: null,
                    ...parameters.filter(p => p.incrementalType === 'cumulative').reduce((acc, param) => {
                        acc[param.parameterName + "Sum"] = {
                            $sum: {
                                $add: [
                                    {$ifNull: [`$data.defaultParams.${param.parameterName}`, 0]},
                                    {$ifNull: [`$data.customParams.${param.parameterName}`, 0]}
                                ]
                            }
                        };
                        return acc;
                    }, {})
                }
            };

            let projectStage = {
                $project: {
                    _id: 0,
                    ...parameters.filter(p => p.incrementalType === 'cumulative').reduce((acc, param) => {
                        acc[param.parameterName + "Sum"] = 1;
                        return acc;
                    }, {})
                }
            };

            let pipeline = [initialMatchStage, groupStage, projectStage];

            return pipeline;
        }

        if (limit) {
            console.log('limit', limit);
            function buildMatchExpression(condition) {
                let expressions = [];

                if (condition.all) {
                    expressions = expressions.concat(condition.all.map(clause => {
                        if (clause.all || clause.any) {
                            return buildMatchExpression(clause);
                        } else {
                            return clauseToMatch(clause);
                        }
                    }));
                }

                if (condition.any) {
                    expressions.push({
                        $or: condition.any.map(clause => {
                            if (clause.all || clause.any) {
                                return buildMatchExpression(clause);
                            } else {
                                return clauseToMatch(clause);
                            }
                        })
                    });
                }

                return expressions.length === 1 ? expressions[0] : {$and: expressions};
            }

            function clauseToMatch(clause) {
                const param = parameters.find(p => p.parameterName === clause.fact);
                let expression = {};

                if (param.incrementalType === "cumulative" && clause.operator === "greaterThanInclusive") {
                    expression = {fact: clause.fact, operator: clause.operator, value: clause.value};
                } else {
                    expression = {
                        $or: [
                            {[`data.defaultParams.${clause.fact}`]: clause.value},
                            {[`data.customParams.${clause.fact}`]: clause.value}
                        ]
                    };
                }
                return expression;
            }

            let matchConditions = buildMatchExpression(businessLogic);
            matchConditions = Array.isArray(matchConditions) ? matchConditions : [matchConditions];

            let initialMatchStage = {$match: {userId: userId}};

// Sort stage by _id in descending order
            let sortStage = {$sort: {_id: -1}};

// Limit stage, adjust the number as per your requirement
            let limitStage = {$limit: limit};

            let groupStage = {
                $group: {
                    _id: null,
                    ...parameters.filter(p => p.incrementalType === 'cumulative').reduce((acc, param) => {
                        acc[param.parameterName + "Sum"] = {
                            $sum: {
                                $add: [
                                    {$ifNull: [`$data.defaultParams.${param.parameterName}`, 0]},
                                    {$ifNull: [`$data.customParams.${param.parameterName}`, 0]}
                                ]
                            }
                        };
                        return acc;
                    }, {})
                }
            };

            let projectStage = {
                $project: {
                    _id: 0,
                    ...parameters.filter(p => p.incrementalType === 'cumulative').reduce((acc, param) => {
                        acc[param.parameterName + "Sum"] = 1;
                        return acc;
                    }, {})
                }
            };

// Incorporate sort and limit stages before the group stage
            let pipeline = [initialMatchStage, sortStage, limitStage, groupStage, projectStage];

            return pipeline;
        }


    }
})
