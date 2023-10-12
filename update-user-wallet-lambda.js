const Sequelize = require('sequelize');
const {MongoClient} = require('mongodb');
const {v4: uuidv4} = require('uuid');
const {QueryTypes} = require("sequelize");
const {Engine} = require('json-rules-engine');

// exports.handler = async (event) => {
//     // Extract eventId and projectId from the event
//     const {eventId, projectId, parameterIds, userId, paramDetails} = event; // You should replace this with the actual way to extract these values from the event
//
//     // Set up Sequelize connection
//     const sequelize = new Sequelize(
//         'gamehub_dev',
//         'gamehub_admin',
//         'Dirtcube2019',
//         {
//             host: 'gamehubdev.cx8tjkw161jy.ap-south-1.rds.amazonaws.com',
//             dialect: 'postgres',
//             port: 5432,
//             logging: false,
//         }
//     );
//
//     // Set up Mongoose connection
//     const mongoURI = 'mongodb+srv://yashh:Yashkadam1234@gamestarz-dev.2tsh39e.mongodb.net/gamehub_dev?retryWrites=true&w=majority'; // Replace placeholders
//     const client = new MongoClient(mongoURI, {
//         useNewUrlParser: true,
//         useUnifiedTopology: true,
//     });
//
//
//     try {
//         await client.connect();
//         const db = client.db();
//
//         const taskParametersCollection = db.collection('taskparameters'); // Use your collection name
//         const tasks = await taskParametersCollection.find({
//             eventId: eventId, // Use the retrieved event's ID
//             projectId: projectId, // Use the provided projectId
//             "parameters": {
//                 $elemMatch: {
//                     "parameterId": {
//                         $in: parameterIds
//                     }
//                 }
//             }
//         }).toArray();
//
//         console.log('tasks', tasks);
//
//         for (let task of tasks) {
//             if (task.parameters.length) {
//                 for (let param of task.parameters) {
//                     if (param.incrementalType === 'one-shot') {
//
//                     }
//
//                     if (param.incrementalType === 'cumulative') {
//                         console.log('task is cumulative')
//                         const userUpdateWalletCollection = db.collection('userupdatewallets');
//                         const pipeline = [
//                             {
//                                 $match: {
//                                     userId: userId
//                                 }
//                             },
//                             {
//                                 $project: {
//                                     _id: 0,
//                                     sum: {$ifNull: [`$data.${param.parameterName}`, 0]}
//                                 }
//                             },
//                             {
//                                 $group: {
//                                     _id: null,
//                                     totalSum: {$sum: "$sum"}
//                                 }
//                             }
//                         ];
//
//                         const result = await userUpdateWalletCollection.aggregate(pipeline).toArray();
//
//                         console.log("Total sum:", result[0].totalSum);
//                         if (result[0].totalSum) {
//                             paramDetails[param.parameterName] = result[0].totalSum
//                         }
//                     }
//                 }
//                 const dbTask = await sequelize.query(`select * from tasks where id='${task.taskId}' limit 1;`, {
//                     type: QueryTypes.SELECT,
//                     nest: true
//                 });
//
//                 if (dbTask) {
//                     let ruleEngine = new Engine();
//                     ruleEngine.addRule({
//                         conditions: dbTask[0].business_logic,
//                         name: 'test',
//                         event: '',
//                         onFailure: () => {
//                             console.log('Job is failed')
//                         },
//                         onSuccess: async () => {
//                             console.log('Job is passed');
//
//                             const dbTaskBus = await sequelize.query(`select * from task_bus
// where user_id='${userId}' and project_id='${projectId}' and task_id='${task.taskId}' limit 1;`, {
//                                 type: QueryTypes.SELECT,
//                                 nest: true,
//                                 raw: true
//                             });
//
//                             if (!dbTaskBus || !dbTaskBus.length) {
//                                 await sequelize.query(`insert into task_bus(id, status, meta, project_id, user_id, task_id, task_group_id, active, archive, created_at, updated_at)
// values (uuid_generate_v4(), 'created', null, '${projectId}', '${userId}', '${task.taskId}', null, true, false, now(), now())`, {
//                                     type: QueryTypes.INSERT,
//                                     nest: true
//                                 });
//                             }
//                         }
//                     });
//                     await ruleEngine.run(paramDetails);
//                     ruleEngine.stop();
//                 }
//             }
//         }
//
//         return {
//             statusCode: 200,
//             body: JSON.stringify({tasks}),
//         };
//     } finally {
//         // Close Mongoose connection
//         await client.close();
//
//         // Close Sequelize connection
//         await sequelize.close();
//     }
// };

async function test(event) {
    // Extract eventId and projectId from the event
    const {eventId, projectId, parameterIds, userId, paramDetails, levelSystemDetails} = event; // You should replace this with the actual way to extract these values from the event

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

        const levelMatchArray = levelSystemDetails.map(detail => ({
            "levelSystemLevelDetails": {
                "$elemMatch": {
                    "levelSystemId": detail.levelSystemId,
                    "level": detail.level
                }
            }
        }));



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

        const tasks = await taskParametersCollection.find({
            eventId: "8bee1e9d-4d18-414c-9972-c21380e8683b", // Use the retrieved event's ID
            projectId: "696bce3a-b9da-4356-8104-94d1bf2f3e33", // Use the provided projectId
            $or: [
                { $and: levelMatchArray },
                { levelSystemLevelDetails: { $size: 0 } },
                { levelSystemLevelDetails: { $exists: false } }
            ],
            "parameters": {
                $elemMatch: {
                    "parameterId": {
                        $in: parameterIds
                    }
                }
            },
        }).toArray();

        console.log('tasks', tasks);

        for (let task of tasks) {
            const utsc = db.collection('usertaskstatus');
            let taskValidationInit = false;
            if (task.parameters.length) {
                for (let param of task.parameters) {
                    // Task is one time
                    if (!task.isRecurring) {
                        const dbTaskBus = await sequelize.query(`select * from task_bus where task_id='${task.taskId}' and user_id='${userId}' limit 1;`, {
                            type: QueryTypes.SELECT,
                            nest: true,
                            raw: true
                        });

                        console.log('dbTaskBus', dbTaskBus);

                        if ((!dbTaskBus || !dbTaskBus.length) && param.incrementalType === 'cumulative') {
                            taskValidationInit = true
                            const userUpdateWalletCollection = db.collection('userupdatewallets');
                            const pipeline = await getAggregateQuery({
                                param,
                                userId,
                                limit: param.noOfRecords || null,
                                startTime: param.startTime || null,
                                endTime: param.endTime || null
                            });
                            console.log('pipeline', pipeline);
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

                        const userUpdateWalletCollection = db.collection('userupdatewallets');

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

                            const userUpdateWalletCollection = db.collection('userupdatewallets');
                            const pipeline = getAggregateQuery({
                                param,
                                userId,
                                limit: param.noOfRecords || null,
                                startTime: param.startTime || null,
                                endTime: param.endTime || null
                            });
                            const result = pipeline ? await userUpdateWalletCollection.aggregate(pipeline).toArray() : [];
                            console.log('result', result);
                            if (result.length && result[0].totalSum) {
                                paramDetails[param.parameterName] = result[0].totalSum
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
                        console.log('task passed')
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
                        })
                    }
                });
                await ruleEngine.run(paramDetails);
                ruleEngine.stop();
            }
        }

        return {
            statusCode: 200,
            body: JSON.stringify({tasks}),
        };
    } finally {
        // Close Mongoose connection
        await client.close();

        // Close Sequelize connection
        await sequelize.close();
    }
}


test(
    {
        eventId: "8bee1e9d-4d18-414c-9972-c21380e8683b",
        parameterIds: ["7d48fdf6-9e77-4fb8-bc07-238dd65ca439", "c986557b-39eb-4948-8a95-fddfa1e7048c",
            "009bb046-9eb6-4338-be38-fe025554d368", "51485ba1-6f83-4e56-a314-fc2fce7456fc"],
        projectId: "696bce3a-b9da-4356-8104-94d1bf2f3e33",
        levelSystemLevelIds: ["9fafecb2-0dd1-4d97-8db2-78d321d93528",
            "50cd0884-3628-4abb-bda2-f155da1266bb"],
        userId: "18e1b5bc-ccb7-4668-b94a-ec80a19ac627",
        paramDetails: {"kills": 2000}
    });

function getAggregateQuery({param, userId, limit, startTime, endTime}) {
    console.log('startTime', startTime);
    console.log('endTime', endTime);
    if (!limit && !startTime && !endTime) {
        return [
            {
                $match: {
                    userId: userId
                }
            },
            {
                $project: {
                    _id: 0,
                    sum: {$ifNull: [`$data.${param.parameterName}`, 0]}
                }
            },
            {
                $group: {
                    _id: null,
                    totalSum: {$sum: "$sum"}
                }
            }
        ];
    }

    if (limit) {
        return [
            {
                $match: {
                    userId: userId
                }
            },
            {
                $project: {
                    _id: 0,
                    sum: {$ifNull: [`$data.${param.parameterName}`, 0]}
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
                    totalSum: {$sum: "$sum"}
                }
            }
        ];
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
