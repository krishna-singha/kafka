// ride-requests John North
// driver-location Mike South
// payment-events John completed
// notifications Alice RideStarted

const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer();

  try {
    console.log("Producer connecting...");
    await producer.connect();
    console.log("Producer connected");

    console.log(`
Format:
<topic> <name> <value>

Examples:
ride-requests John North
driver-location Mike South
payment-events John completed
notifications Alice RideStarted
    `);

    rl.setPrompt("> ");
    rl.prompt();

    rl.on("line", async (input) => {
      try {
        const [topic, name, value] = input.trim().split(" ");

        if (!topic || !name || !value) {
          console.log("Invalid input");
          rl.prompt();
          return;
        }

        // Dynamic partition strategy
        let partition = 0;

        if (value.toLowerCase() === "north") {
          partition = 0;
        } else if (value.toLowerCase() === "south") {
          partition = 1;
        } else {
          partition = 0;
        }

        await producer.send({
          topic,
          messages: [
            {
              partition,
              key: name,
              value: JSON.stringify({
                name,
                value,
                timestamp: new Date().toISOString(),
              }),
            },
          ],
        });

        console.log(`Message sent to [${topic}]`);
        rl.prompt();
      } catch (err) {
        console.error("Send failed:", err.message);
        rl.prompt();
      }
    });

    rl.on("close", async () => {
      console.log("\nDisconnecting producer...");
      await producer.disconnect();
      console.log("Producer disconnected");
      process.exit(0);
    });
  } catch (err) {
    console.error("Error:", err);
  }
}

init();
