const express = require("express");
const mongoose = require("mongoose");
const Chance = require("chance"); // Use the Chance library for generating random data
const fs = require("fs");
const path = require("path");
const cluster = require("cluster");
const ncpu = require("os").cpus().length;
const JSONStream = require("JSONStream");

require("dotenv").config(); // Load environment variables from .env

const app = express();
const PORT = process.env.PORT || 3000;
const recordCount = 1000000;
const recordLimit = 10;
const dataDirectory = path.join(__dirname, "data");

const schemaObject = {};
for (let i = 1; i <= 100; i++) {
  schemaObject[`field${i}`] = String; // All fields are defined as String
}
const userSchema = new mongoose.Schema(schemaObject);

const User = mongoose.model("new-users", userSchema);

// MongoDB Connection
mongoose
  .connect(process.env.MONGO_URL, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  })
  .then(() => {
    console.log("Connected to MongoDB");
  })
  .catch((error) => {
    console.error("Error connecting to MongoDB:", error);
  });

// Define your routes and middleware here
app.get("/populate", async (req, res) => {
  const chance = new Chance();
  const schemaObject = {};
  for (let i = 1; i <= 100; i++) {
    schemaObject[`field${i}`] = String; // All fields are defined as String
  }
  const userSchema = new mongoose.Schema(schemaObject);

  const User = mongoose.model("new-users", userSchema);

  const numberOfDocuments = 1000000; // Change this to the desired number of documents

  try {
    for (let i = 0; i < numberOfDocuments; i++) {
      const newUserObject = {};
      for (let i = 1; i <= 100; i++) {
        newUserObject[`field${i}`] = chance.word();
      }
      const user = new User(newUserObject);

      await user.save();
      console.log(`Inserted user ${i + 1}`);
    }
    console.log("All users inserted successfully.");
  } catch (error) {
    console.error("Error inserting users:", error);
  }
});

app.get("/data", async (req, res) => {
  try {
    const dataDirectory = path.join(__dirname, "data"); // Define your data directory path

    if (!fs.existsSync(dataDirectory)) {
      fs.mkdirSync(dataDirectory);
    }

    const totalBatches = Math.ceil(recordCount / recordLimit);
    let completedBatches = 0; // Counter for completed batches

    let i = 0;
    while (i < recordCount) {
      const outputStream = fs.createWriteStream(
        path.join(dataDirectory, `output${i + 1}.json`)
      );
      const cursor = User.find()
        .skip(i)
        .limit(recordLimit)
        .sort({ _id: -1 })
        .cursor();

      // Create a JSONStream serializer to convert documents to JSON strings
      const jsonStream = JSONStream.stringify();

      // Pipe the cursor to the JSONStream and then pipe the JSONStream to the write stream
      cursor.pipe(jsonStream).pipe(outputStream);

      // Handle errors for the cursor and stream
      cursor.on("error", (err) => {
        console.error("Cursor error:", err);
      });

      jsonStream.on("error", (err) => {
        console.error("JSONStream error:", err);
      });

      // Use an async function to await the 'finish' event of the JSONStream
      await new Promise((resolve) => {
        jsonStream.on("finish", () => {
          completedBatches++;
          i += recordLimit; // Increment i to fetch the next batch

          if (completedBatches === totalBatches) {
            // All batches are completed, resolve the promise
            console.log("Data extraction complete.");
            resolve();
          }
        });
      });
    }

    return res.status(200).json({
      success: true,
      message: "Data extraction complete",
    });
  } catch (err) {
    console.log(err);
    return res.status(500).json({
      err: err.message,
    });
  }
});

app.get("/multithread/data", async (req, res) => {
  try {
    if (cluster.isPrimary) {
      console.log("-_-");
      // this is master thread
      console.log("number of cpus:", ncpu);
      for (let i = 0; i < ncpu; i++) {
        const worker = cluster.fork();

        worker.on("message", (data) => {
          console.log(data);
        });

        worker.send({ message: "HI FROM MASTER NODE" });
      }
    } else {
      console.log("************************");
      // this is worker thread
      process.on("message", (data) => {
        process.send({ message: "HI FROM WORKER", data });
      });
    }
    return res.status(200).json({
      success: true,
      message: "data added successfully",
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: err.message,
    });
  }
});
// define a route that reads all documents
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
