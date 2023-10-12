const Sequelize = require('sequelize');
const {QueryTypes} = Sequelize;
const {v4: uuidv4} = require('uuid');

async function createMatchSession(payload) {

    payload = {
        userId: "3c1da295-17ee-4180-b899-4868677ed351",
        matchId: "62ad5fe7-8e58-4b49-b841-bb0c2fb97ab3",
        outcome: 7000
    }


    const sequelize = new Sequelize(
        'gamehub_dev',
        'gamehub_admin',
        'Dirtcube2019',
        {
            host: 'gamehubdev.cx8tjkw161jy.ap-south-1.rds.amazonaws.com',
            dialect: 'postgres',
            port: 5432,
            logging: false, // To ensure SQL logging doesn't clutter your CloudWatch logs
        },
    );

    const newMatchSession = await sequelize.query(
        `INSERT INTO "MatchSessionTest" ("id","outcome","user_id","match_id",
"active","archive","created_at","updated_at") 
VALUES (:id, :outcome, :userId, :matchId, :active, :archive, :currentTime, :currentTime) 
RETURNING "id","outcome","user_id","match_id","active","archive","created_at","updated_at";`,
        {
            type: QueryTypes.INSERT,
            nest: true,
            replacements: {
                id: uuidv4(),
                outcome: payload.outcome,
                userId: payload.userId,
                matchId: payload.matchId,
                active: true,
                archive: false,
                currentTime: '2023-07-14 13:46:22.232000 +00:00',
            },
        },
    );

    const sum = await sequelize.query(
        `SELECT sum("outcome") AS 
"sum" FROM "MatchSessionTest" AS "MatchSessionTest" WHERE "MatchSessionTest"."user_id" = :userId AND "MatchSessionTest"."match_id" = :matchId;`,
        {
            type: QueryTypes.SELECT,
            replacements: {userId: payload.userId, matchId: payload.matchId},
        },
    );

    const completedTasks = [];
    const rewards = [];

    const dbTasks = await sequelize.query(
        `SELECT "id", "name", "completed", "step", "event_id"
 AS "eventId", "item_id" AS "itemId", "active", "archive", "created_at" AS "createdAt", "updated_at" AS "updatedAt" FROM "TaskTest" AS "TaskTest" WHERE "TaskTest"."completed" = false;`,
        {nest: true, type: QueryTypes.SELECT},
    );

    for (let task of dbTasks) {
        if (sum[0].sum >= task.step) {
            completedTasks.push({
                userId: payload.userId,
                taskId: task.id,
            });
            rewards.push({
                userId: payload.userId,
                itemId: task.itemId,
            });

            await sequelize.query(
                `UPDATE "TaskTest" SET "completed"=true WHERE "id" = :taskId`,
                {replacements: {taskId: task.id}, type: QueryTypes.UPDATE},
            );
        }
    }

    for (let compTask of completedTasks) {
        await sequelize.query(
            `INSERT INTO "UserAchievementTest" ("id","user_id","task_id","active","archive","created_at","updated_at") 
       VALUES (:id, :userId, :taskId, :active, :archive, :currentTime, :currentTime)
       RETURNING "id","user_id","task_id","active","archive","created_at","updated_at"`,
            {
                replacements: {
                    id: uuidv4(),
                    userId: payload.userId,
                    taskId: compTask.taskId,
                    active: true,
                    archive: false,
                    currentTime: '2023-07-14 13:46:22.232000 +00:00',
                },
                type: QueryTypes.INSERT,
            },
        );
    }

    for (let reward of rewards) {
        await sequelize.query(
            `INSERT INTO "UserInventoryTest" ("id","user_id","item_id","active","archive","created_at","updated_at") 
         VALUES (:id, :userId, :itemId, :active, :archive, :currentTime, :currentTime) 
         RETURNING "id","user_id","item_id","active","archive","created_at","updated_at"`,
            {
                replacements: {
                    id: uuidv4(),
                    userId: payload.userId,
                    itemId: reward.itemId,
                    active: true,
                    archive: false,
                    currentTime: '2023-07-14 13:46:22.232000 +00:00',
                },
                type: QueryTypes.INSERT,
            },
        );
    }

    return newMatchSession;
}
