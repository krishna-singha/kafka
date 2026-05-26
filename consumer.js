// node consumer.js group-1 ride-requests
// node consumer.js group-2 driver-location
// node consumer.js analytics-group analytics-events
// node consumer.js group-1 all



const { kafka } = require("./client");

const group = process.argv[2] || "test-group";
const topic = process.argv[3] || "all";

async function init() {
  const consumer = kafka.consumer({
    groupId: group,
  });

  try {
    console.log("Consumer connecting...");
    await consumer.connect();
    console.log("Consumer connected");

    if (topic === "all") {
      console.log("Subscribing to all topics...");

      await consumer.subscribe({
        topic: /^.*/, // regex → all topics
        fromBeginning: true,
      });

      console.log("Subscribed to all topics");
    } else {
      console.log(`Subscribing to [${topic}]`);

      await consumer.subscribe({
        topic,
        fromBeginning: true,
      });

      console.log(`Subscribed to [${topic}]`);
    }

    console.log("Consumer running...");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const data = JSON.parse(message.value.toString());

          console.log(`
==================================
Topic      : ${topic}
Partition  : ${partition}
Group      : ${group}
Key        : ${message.key?.toString()}
Message    : ${JSON.stringify(data, null, 2)}
Offset     : ${message.offset}
Timestamp  : ${message.timestamp}
==================================
        `);
        } catch {
          console.log(`
Topic      : ${topic}
Partition  : ${partition}
Message    : ${message.value.toString()}
            `);
        }
      },
    });
  } catch (err) {
    console.error("Consumer error:", err);
  }

  process.on("SIGINT", async () => {
    console.log("\nDisconnecting consumer...");
    await consumer.disconnect();
    console.log("Consumer disconnected");
    process.exit(0);
  });
}

init();
