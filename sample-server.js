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
    console.log('                  ')
    console.log('req.body', req.body);
//     // Extract eventId and projectId from the event
    const {eventId, projectId, parameterIds, userId, paramDetails, levelSystemDetails, collectionName} = req.body; // You should replace this with the actual way to extract these values from the event

    // Set up Sequelize connection
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


    try {
        await client.connect();
        const db = client.db();

        const taskParametersCollection = db.collection('taskparameters'); // Use your collection name

        // const tasks = await taskParametersCollection.find({
        //     eventId: eventId, // Use the retrieved event's ID
        //     projectId: projectId, // Use the provided projectId
        //     $or: [
        //         {
        //             levelSystemLevelIds: {
        //                 $all: levelSystemLevelIds,
        //                 $size: levelSystemLevelIds.length
        //             }
        //         },
        //         {levelSystemLevelIds: {$size: 0}}
        //     ],
        //     "parameters": {
        //         $elemMatch: {
        //             "parameterId": {
        //                 $in: parameterIds
        //             }
        //         }
        //     },
        // }).toArray();
        // const dbLevelSystemLevelMapping =  await sequelize.query(`select * from level`, {
        //     type: QueryTypes.INSERT,
        //     nest: true
        // });


        // const levelSystemLevelInput = [
        //     {
        //         "levelSystemId": "49d5413b-e087-4cae-9433-99abf3101b83",
        //         "level": 1
        //     },
        //     {
        //         "levelSystemId": "49d5413b-e087-4cae-9433-99abf3101b83",
        //         "level": 3
        //     }
        // ];

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
                "parameters": {
                    $elemMatch: {
                        "parameterId": {
                            $in: parameterIds
                        }
                    }
                },
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

        console.log('tasks', tasks);

        for (let task of tasks) {
            const utsc = db.collection('usertaskstatus');
            let taskValidationInit = false;

            if (!task.parameters || !task.parameters.length) {
                console.log('task passed because no params found');
                await sequelize.query(`insert into task_bus(id, status, meta, project_id, user_id, task_id, task_group_id, active, archive, created_at, updated_at)
values (uuid_generate_v4(), 'created', null, '${projectId}', '${userId}', '${task.taskId}', null, true, false, now(), now())`, {
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

                if (task.taskGroupId) {
                    const noOfConfigTasks = await sequelize.query(`select id from tasks where task_group_id='${task.taskGroupId}';`, {
                        type: QueryTypes.SELECT,
                        nest: true,
                        raw: true
                    });
                    console.log('result', noOfConfigTasks);

                    const ids = noOfConfigTasks.map(item => `'${item.id}'`).join(', ');

                    console.log('ids', ids);

                    const noOfTasksCompleted = await sequelize.query(`select count(*) from task_bus where task_id in (${ids});`, {type: QueryTypes.SELECT});
                    console.log('noOfTasksCompleted', noOfTasksCompleted[0].count);
                    console.log('noOfConfigTasks', noOfConfigTasks.length);

                    if (noOfTasksCompleted[0].count >= noOfConfigTasks.length) {
                        // Task group is completed
                        await sequelize.query(`insert into task_bus(id, status, meta, project_id, user_id, task_id, task_group_id, active, archive, created_at, updated_at)
                     values (uuid_generate_v4(), 'created', null, '${projectId}', '${userId}', null, '${task.taskGroupId}', true, false, now(), now())`, {
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

            if (task.parameters && task.parameters.length) {
                let shouldEvaluate = true

                for (let param of task.parameters) {
                    console.log('task', task.taskId);
                    console.log('task.eventId', task.eventId);
                    console.log('param', param);
                    // Task is one time
                    if (!task.isRecurring) {
                        const dbTaskBus = await sequelize.query(`select * from task_bus where task_id='${task.taskId}' and user_id='${userId}' limit 1;`, {
                            type: QueryTypes.SELECT,
                            nest: true,
                            raw: true
                        });

                        console.log('dbTaskBus', dbTaskBus, 'and param.incrementalType', param.incrementalType);

                        if (dbTaskBus.length) {
                            shouldEvaluate = false;
                        }

                        if ((!dbTaskBus || !dbTaskBus.length) && param.incrementalType === 'cumulative') {
                            taskValidationInit = true
                            const userUpdateWalletCollection = db.collection(collectionName);
                            const pipeline = await getAggregateQuery({
                                parameters : task.parameters,
                                userId,
                                limit: param.noOfRecords || null,
                                startTime: param.startTime || null,
                                endTime: param.endTime || null
                            });
                            console.log('query', JSON.stringify(pipeline));
                            console.log('collectionName', collectionName);
                            const result = await userUpdateWalletCollection.aggregate(pipeline).toArray();
                            console.log('result', result);
                            if (result.length && result[0].totalSum) {
                                paramDetails[param.parameterName] = result[0].totalSum
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
                                // console.log('dbDoc', dbDoc.length);
                                // console.log('param.limit', param.noOfRecords);
                                // console.log('dbDoc.length % param.limit', dbDoc.length % param.noOfRecords);
                                const modResult = dbDoc.length ? dbDoc.length % param.noOfRecords : param.noOfRecords;
                                // console.log('modResult', modResult);
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
                                parameters : task.parameters,
                                userId,
                                limit: param.noOfRecords || null,
                                startTime: param.startTime || null,
                                endTime: param.endTime || null
                            });
                            console.log('collectionName', collectionName);
                            const result = pipeline ? await userUpdateWalletCollection.aggregate(pipeline).toArray() : [];
                            console.log('result', result);
                            if (result.length && result[0].totalSum) {
                                paramDetails[param.parameterName] = result[0].totalSum
                            }
                        }
                    }
                }

                console.log('paramDetails before update', paramDetails);
                console.log('task.businessLogic', task.businessLogic);
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
                        if (shouldEvaluate) {
                            console.log('task passed');
                            await sequelize.query(`insert into task_bus(id, status, meta, project_id, user_id, task_id, task_group_id, active, archive, created_at, updated_at)
values (uuid_generate_v4(), 'created', null, '${projectId}', '${userId}', '${task.taskId}', null, true, false, now(), now())`, {
                                type: QueryTypes.INSERT,
                                nest: true
                            });

                            await utsc.insertOne({
                                taskId: task.taskId,
                                projectId: projectId,
                                userId: userId,
                                status: 'succeed'
                            });


                            console.log('                                       ');
                            console.log('Making an api call after task is passed')
                            await axios.post('http://localhost:3000/v1/task/grantReward', {
                                userId: userId,
                                eventId: eventId,
                                taskId: task.taskId
                            });

                            if (task.taskGroupId) {
                                console.log('                                       ');
                                console.log('Inside the task group call');
                                const dbTaskGroupBus = await sequelize.query(`select * from task_bus where task_group_id='${task.taskGroupId}' and user_id='${userId}' limit 1;`, {
                                    type: QueryTypes.SELECT,
                                    nest: true,
                                    raw: true
                                });

                                console.log('dbTaskGroupBus', dbTaskGroupBus);

                                if (!dbTaskGroupBus.length) {
                                    const noOfConfigTasks = await sequelize.query(`select id from tasks where task_group_id='${task.taskGroupId}';`, {
                                        type: QueryTypes.SELECT,
                                        nest: true,
                                        raw: true
                                    });
                                    console.log('result', noOfConfigTasks);

                                    const ids = noOfConfigTasks.map(item => `'${item.id}'`).join(', ');

                                    console.log('ids', ids);

                                    const noOfTasksCompleted = await sequelize.query(`select count(*) from task_bus where task_id in (${ids});`, {type: QueryTypes.SELECT});
                                    console.log('noOfTasksCompleted', noOfTasksCompleted[0].count);
                                    console.log('noOfConfigTasks', noOfConfigTasks.length);

                                    console.log('typeof noOfTasksCompleted[0].count' ,typeof noOfTasksCompleted[0].count);
                                    console.log('converted', typeof Number(noOfTasksCompleted[0].count));
                                    console.log('typeof noOfConfigTasks', typeof noOfConfigTasks.length);

                                    console.log('truth', Number(noOfTasksCompleted[0].count) >= noOfConfigTasks.length);

                                    if (Number(noOfTasksCompleted[0].count) >= noOfConfigTasks.length) {
                                        // Task group is completed
                                        await sequelize.query(`insert into task_bus(id, status, meta, project_id, user_id, task_id, task_group_id, active, archive, created_at, updated_at)
                     values (uuid_generate_v4(), 'created', null, '${projectId}', '${userId}', null, '${task.taskGroupId}', true, false, now(), now())`, {
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
                });
                // for (let key in paramDetails) {
                //     paramDetails[key] = paramDetails[key];
                // }
                console.log('paramDetails after update', paramDetails);
                await ruleEngine.run(paramDetails);
                ruleEngine.stop();
            }

        }

        return res.json({success: true})


        // return {
        //     statusCode: 200,
        //     body: JSON.stringify({tasks}),
        // };
    } finally {
        // Close Mongoose connection
        await client.close();

        // Close Sequelize connection
        await sequelize.close();
    }


    function getAggregateQuery({parameters, userId, limit, startTime, endTime}) {
        if (!limit && !startTime && !endTime) {
            const oneShotConditions = parameters.map(param => {
                if (param.incrementalType === 'one-shot') {
                    return {
                        $or: [
                            { [`data.defaultParams.${param.parameterName}`]: param.value },
                            { [`data.customParams.${param.parameterName}`]: param.value }
                        ]
                    };
                }
                return null;
            }).filter(Boolean);

            const sumLogic = parameters.map(param => {
                if (param.incrementalType === 'cumulative') {
                    return {
                        $add: [
                            { $ifNull: [`$data.defaultParams.${param.parameterName}`, 0] },
                            { $ifNull: [`$data.customParams.${param.parameterName}`, 0] }
                        ]
                    };
                }
                return null;
            }).filter(Boolean);

            let initialMatch = {
                userId: userId
            };

            if (oneShotConditions.length > 0) {
                initialMatch.$and = [
                    { userId: userId },
                    { $or: oneShotConditions }
                ];
            }

            return [
                {
                    $match: initialMatch
                },
                {
                    $project: {
                        _id: 0,
                        sum: {
                            $sum: sumLogic
                        }
                    }
                },
                {
                    $group: {
                        _id: null,
                        totalSum: { $sum: "$sum" }
                    }
                }
            ];
        }

        if (limit) {
            const oneShotConditions = parameters.map(param => {
                if (param.incrementalType === 'one-shot') {
                    return {
                        $or: [
                            { [`data.defaultParams.${param.parameterName}`]: param.value },
                            { [`data.customParams.${param.parameterName}`]: param.value }
                        ]
                    };
                }
                return null;
            }).filter(Boolean);

            const sumLogic = parameters.map(param => {
                if (param.incrementalType === 'cumulative') {
                    return {
                        $add: [
                            { $ifNull: [`$data.defaultParams.${param.parameterName}`, 0] },
                            { $ifNull: [`$data.customParams.${param.parameterName}`, 0] }
                        ]
                    };
                }
                return null;
            }).filter(Boolean);

            let initialMatch = {
                userId: userId
            };

            if (oneShotConditions.length > 0) {
                initialMatch.$and = [
                    { userId: userId },
                    { $or: oneShotConditions }
                ];
            }

            return [
                {
                    $match: initialMatch
                },
                {
                    $project: {
                        _id: 0,
                        sum: {
                            $sum: sumLogic
                        }
                    }
                },
                {
                    $sort: {_id: -1}
                },
                {
                    $limit: limit
                },
                {
                    $group: {
                        _id: null,
                        totalSum: { $sum: "$sum" }
                    }
                }
            ];


            // return [
            //     {
            //         $match: {
            //             userId: userId
            //         }
            //     },
            //     {
            //         $project: {
            //             _id: 0,
            //             sum: {$ifNull: [`$data.${param.parameterName}`, 0]}
            //         }
            //     },
            //     {
            //         $sort: {_id: -1}
            //     },
            //     {
            //         $limit: limit
            //     },
            //     {
            //         $group: {
            //             _id: null,
            //             totalSum: {$sum: "$sum"}
            //         }
            //     }
            // ];
        }

        // if (startTime || endTime) {
        //     let dateFilter = {};
        //     if (startTime) {
        //         dateFilter.$gte = new Date(startTime);
        //     }
        //     if (endTime) {
        //         dateFilter.$lte = new Date(endTime);
        //     }
        //
        //     console.log('dateFilter', dateFilter);
        //
        //
        //     let matchStage = {
        //         $match: {
        //             userId: userId
        //         }
        //     };
        //
        //     const pipeline = [
        //         matchStage,
        //         {
        //             $unwind: "$parameters"
        //         },
        //         {
        //             $match: {
        //                 "parameters.parameterName": param.parameterName
        //             }
        //         }
        //     ];
        //
        //     if (Object.keys(dateFilter).length > 0) {
        //         pipeline.push({
        //             $match: {
        //                 createdAt: dateFilter
        //             }
        //         });
        //     }
        //
        //     pipeline.push(
        //         {
        //             $project: {
        //                 _id: 0,
        //                 sum: {$ifNull: [`$parameters.${param.parameterName}`, 0]}
        //             }
        //         },
        //         {
        //             $group: {
        //                 _id: null,
        //                 totalSum: {$sum: "$sum"}
        //             }
        //         }
        //     );
        //     return pipeline
        // }

        if (startTime || endTime) {
            let dateFilter = {};
            if (startTime) {
                dateFilter.$gte = new Date(startTime);
            }
            if (endTime) {
                dateFilter.$lte = new Date(endTime);
            }

            console.log('dateFilter', dateFilter);

            let matchStage = {
                $match: {
                    userId: userId,
                    createdAt: dateFilter  // Added date filter here
                }
            };

            const pipeline = [
                matchStage,
                {
                    $project: {
                        _id: 0,
                        sum: {$ifNull: [`$data.${param.parameterName}`, 0]}  // Adjusted field name
                    }
                },
                {
                    $group: {
                        _id: null,
                        totalSum: {$sum: "$sum"}
                    }
                }
            ];
            console.log('query', JSON.stringify(pipeline));
            return pipeline
        }
    }
})
