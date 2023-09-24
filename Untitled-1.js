const cluster = require("cluster");
const os = require("os");
const { MongoClient } = require("mongodb");
const fs = require("fs");
const path = require("path");
const dataDirectory = path.join(__dirname, "data");

const TOTAL_RECORDS = 1000000;
const batchSize = 5000;
let allotedRecords = 0;
// Determine the number of CPU cores
const ncpu = os.cpus().length;
let activeWorkers = 0;

console.time("TotalScriptExecutionTime");

const workerMessageHandler = (worker, message) => {
  // append message.results in file named with worker.id
  // if (!fs.existsSync(dataDirectory)) {
  //   fs.mkdirSync(dataDirectory);
  // }
  // fs.writeFile(
  //   path.join(dataDirectory, `batch${Math.random()}.json`), // Specify the full file path
  //   JSON.stringify(message.data, null, 2),
  //   (err) => {
  //     if (err) {
  //       console.log(err.message);
  //     }

  //     if (allotedRecords >= TOTAL_RECORDS) {
  //       // All workers are done, and there are no records remaining
  //       activeWorkers--;

  //       if (!activeWorkers) {
  //         console.timeEnd("TotalScriptExecutionTime");
  //         console.timeLog("TotalScriptExecutionTime");

  //         // Kill all workers
  //         for (const id in cluster.workers) {
  //           cluster.workers[id].kill();
  //         }
  //       }
  //     } else {
  //       // If there are records remaining, assign more work to this worker
  //       const recordsToFetch = Math.min(
  //         TOTAL_RECORDS - allotedRecords,
  //         batchSize
  //       );
  //       allotedRecords += recordsToFetch;
  //       console.log(allotedRecords);
  //       worker.send({
  //         message: "FETCH",
  //         data: { limit: recordsToFetch, skip: allotedRecords },
  //       });
  //     }
  //   }
  // );
  worker.send({ messgae: "DATA ADDED" });
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
      // Connect to MongoDB using the provided URL
      const client = new MongoClient(
        "mongodb://localhost:27017/large-db-testing"
      );
      await client.connect();

      // Access a specific collection (e.g., 'mycollection') in the database
      // const collection = client.db().collection("new-users");

      // Perform a query to fetch records
      // return (collection
      //   .find()
      //   .skip(data.data.skip)
      //   .limit(data.data.limit)).toArray();

      // const result = await cursor.toArray();

      // console.log(`successfully fetched results, now sending results to main node`, result)
      // Send the fetched records back as a message

      if (!fs.existsSync(dataDirectory)) {
        fs.mkdirSync(dataDirectory);
      }
      // write fields
      fs.writeFile(
        path.join(dataDirectory, `batch${Math.random()}.json`), // Specify the full file path
        JSON.stringify(
          client
            .db()
            .collection("new-users")
            .find()
            .skip(data.data.skip)
            .limit(data.data.limit)
            .toArray()
        ),
        null,
        2
      ),
        (err) => {
          if (err) {
            console.log(err.message);
          }

          if (allotedRecords >= TOTAL_RECORDS) {
            // All workers are done, and there are no records remaining
            activeWorkers--;

            if (!activeWorkers) {
              console.timeEnd("TotalScriptExecutionTime");
              console.timeLog("TotalScriptExecutionTime");

              // Kill all workers
              for (const id in cluster.workers) {
                cluster.workers[id].kill();
              }
            }
          } else {
            // If there are records remaining, assign more work to this worker
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
        };

      process.send({
        message: "Fetched records",
        // data: JSON.stringify(
        //   collection
        //     .find()
        //     .skip(data.data.skip)
        //     .limit(data.data.limit)
        //     .toArray()
        // ),
      });

      // Close the MongoDB connection
      await client.close();
    } catch (error) {
      // Handle any errors that may occur during the process
      console.error("Error:", error.message);
      console.table(data);
      process.send({ message: "Error", error: error.message });
    }
  });
}
