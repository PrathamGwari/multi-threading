const cluster = require("cluster");
const os = require("os");
const { MongoClient } = require("mongodb");
const fs = require("fs");
const path = require("path");
const outputFolder = path.join(__dirname, "data");
const JSONStream = require("JSONStream");

const TOTAL_RECORDS = 1000000;
const batchSize = 100000;
let allotedRecords = 0;
// Determine the number of CPU cores
const ncpu = os.cpus().length;
let activeWorkers = 0;

console.time("TotalScriptExecutionTime");

const workerMessageHandler = (worker, message) => {
  activeWorkers--;

  if (allotedRecords >= TOTAL_RECORDS) {
    // All workers are done, and there are no records remaining
    if (!activeWorkers) {
      console.log('operation completed!')
      console.timeEnd("TotalScriptExecutionTime");
      console.timeLog('Total Time Taken:',"TotalScriptExecutionTime");

      // Kill all workers
      for (const id in cluster.workers) {
        cluster.workers[id].kill();
      }
    }
  } else {
    // If there are records remaining, assign more work to this worker
    activeWorkers++;
    const recordsToFetch = Math.min(TOTAL_RECORDS - allotedRecords, batchSize);
    allotedRecords += recordsToFetch;
    console.log(allotedRecords);
    worker.send({
      message: "FETCH",
      data: { limit: recordsToFetch, skip: allotedRecords },
    });
  }
};

if (cluster.isMaster) {
  // This is the master thread
  console.log("Number of CPUs:", ncpu);
  console.log(
    `Fetching total ${TOTAL_RECORDS} documents \nBatch Size - ${batchSize} documents per request...`
  );

  for (let i = 0; i < ncpu; i++) {
    if (allotedRecords < TOTAL_RECORDS) {
      const worker = cluster.fork();
      activeWorkers++;

      worker.on("message", (data) => {
        workerMessageHandler(worker, data);
      });

      // Assign the initial batch of work
      const recordsToFetch = Math.min(
        TOTAL_RECORDS - allotedRecords,
        batchSize
      );
      allotedRecords += recordsToFetch;
      console.log(allotedRecords);
      worker.send({
        message: "FETCH",
        data: { limit: recordsToFetch, skip: allotedRecords },
      });
    }
  }
} else {
  // This is a worker thread
  process.on("message", async (data) => {
    try {
      if (!fs.existsSync(outputFolder)) {
        fs.mkdirSync(outputFolder);
      }
      // Connect to MongoDB using the provided URL
      const client = new MongoClient(
        "mongodb://0.0.0.0:27017/large-db-testing"
      );
      await client.connect();

      const mongoResultStream = JSONStream.stringify();
      const outputFile = `batch${Math.random()}.json`;
      const outputFilePath = path.join(outputFolder, outputFile);
      const outputStream = fs.createWriteStream(outputFilePath);

      mongoResultStream.pipe(outputStream);

      const collection = client.db().collection("new-users");
      collection
        .find()
        .skip(data.data.skip)
        .limit(data.data.limit)
        .stream()
        .pipe(mongoResultStream);

      // When the stream is finished, close the write stream
      mongoResultStream.on("end", () => {
        outputStream.end();
        client.close(); // Close the MongoDB connection

        process.send({
          message: "Fetched records",
        });
      });
    } catch (error) {
      // Handle any errors that may occur during the process
      console.error("Error:", error.message);
      console.table(data);
      process.send({ message: "Error", error: error.message });
    }
  });
}
