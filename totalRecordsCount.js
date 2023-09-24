const fs = require("fs");
const path = require("path");

const dataDirectory = path.join(__dirname, "data");

// Function to read and count JSON records in a file
const countRecordsInFile = (filePath) => {
  try {
    const fileData = fs.readFileSync(filePath, "utf8");
    const jsonData = JSON.parse(fileData);
    return jsonData.length;
  } catch (error) {
    console.error(`Error reading file ${filePath}: ${error.message}`);
    return 0;
  }
};

// Function to get a list of JSON files in a directory
const getJsonFilesInDirectory = (dirPath) => {
  return fs
    .readdirSync(dirPath)
    .filter((filename) => filename.endsWith(".json"))
    .map((filename) => path.join(dirPath, filename));
};

// Main function to calculate the total number of records
const calculateTotalRecords = () => {
  const jsonFiles = getJsonFilesInDirectory(dataDirectory);
  let totalRecords = 0;

  for (const filePath of jsonFiles) {
    const recordsInFile = countRecordsInFile(filePath);
    totalRecords += recordsInFile;
  }

  return totalRecords;
};

// Calculate and display the total number of records
const totalRecords = calculateTotalRecords();
console.log(`Total number of records: ${totalRecords}`);
