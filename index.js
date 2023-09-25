const express = require("express");
const mongoose = require("mongoose");
const Chance = require("chance");
const fs = require("fs");
const path = require("path");
const cluster = require("cluster");
const os = require("os");
const JSONStream = require("JSONStream");

require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3000;
const recordCount = 1000000;
const recordLimit = 10;
const dataDirectory = path.join(__dirname, "data");

// Define your Mongoose model outside of the route handlers
const schemaObject = {};
for (let i = 1; i <= 100; i++) {
  schemaObject[`field${i}`] = String;
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

  const numberOfDocuments = 1000000;

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

  return res.status(200).json({
    success: true,
    message: "Data population complete",
  });
});

app.get("/data", async (req, res) => {
  try {
    if (!fs.existsSync(dataDirectory)) {
      fs.mkdirSync(dataDirectory);
    }

    const totalBatches = Math.ceil(recordCount / recordLimit);
    let completedBatches = 0;

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

      const jsonStream = JSONStream.stringify();

      cursor.pipe(jsonStream).pipe(outputStream);

      cursor.on("error", (err) => {
        console.error("Cursor error:", err);
      });

      jsonStream.on("error", (err) => {
        console.error("JSONStream error:", err);
      });

      await new Promise((resolve) => {
        jsonStream.on("finish", () => {
          completedBatches++;
          i += recordLimit;

          if (completedBatches === totalBatches) {
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
    console.error(err);
    return res.status(500).json({
      err: err.message,
    });
  }
});

app.get("/multithread/data", async (req, res) => {
  try {
    if (cluster.isPrimary) {
      console.log("Number of CPUs:", os.cpus().length);
      for (let i = 0; i < os.cpus().length; i++) {
        const worker = cluster.fork();

        worker.on("message", (data) => {
          console.log(data);
        });

        worker.send({ message: "HI FROM MASTER NODE" });
      }
    } else {
      console.log("Worker thread");
      process.on("message", (data) => {
        process.send({ message: "HI FROM WORKER", data });
      });
    }

    return res.status(200).json({
      success: true,
      message: "Data processing started",
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: err.message,
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
