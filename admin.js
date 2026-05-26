const { kafka } = require("./client");

async function init() {
  const admin = kafka.admin();

  try {
    console.log("Admin connecting...");
    await admin.connect();
    console.log("Admin connected");

    console.log("Creating topics...");

    await admin.createTopics({
      topics: [
        {
          topic: "rider-updates",
          numPartitions: 2,
          replicationFactor: 1,
        },
        {
          topic: "driver-location",
          numPartitions: 4,
          replicationFactor: 1,
        },
        {
          topic: "ride-requests",
          numPartitions: 3,
          replicationFactor: 1,
        },
        {
          topic: "ride-status",
          numPartitions: 3,
          replicationFactor: 1,
        },
        {
          topic: "payment-events",
          numPartitions: 2,
          replicationFactor: 1,
        },
        {
          topic: "notifications",
          numPartitions: 2,
          replicationFactor: 1,
        },
        {
          topic: "analytics-events",
          numPartitions: 5,
          replicationFactor: 1,
        },
        {
          topic: "audit-logs",
          numPartitions: 1,
          replicationFactor: 1,
        },
      ],
      waitForLeaders: true,
    });

    console.log("Topics created successfully");
  } catch (err) {
    console.error("Error creating topics:", err);
  } finally {
    console.log("Disconnecting admin...");
    await admin.disconnect();
    console.log("Admin disconnected");
  }
}

init();
