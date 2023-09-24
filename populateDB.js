const mongoose = require('mongoose');
const faker = require('faker');

mongoose.connect('mongodb://localhost:27017/large-db-testing', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

const userSchema = new mongoose.Schema({
  firstName: String,
  lastName: String,
  email: String,
  // Add other user fields as needed
});

const User = mongoose.model('User', userSchema);

const numberOfDocuments = 100; // Change this to the desired number of documents

const insertRandomUsers = async () => {
  try {
    for (let i = 0; i < numberOfDocuments; i++) {
      const user = new User({
        firstName: faker.name.firstName(),
        lastName: faker.name.lastName(),
        email: faker.internet.email(),
        // Add other user data fields as needed
      });

      await user.save();
      console.log(`Inserted user ${i + 1}`);
    }
    console.log('All users inserted successfully.');
  } catch (error) {
    console.error('Error inserting users:', error);
  } finally {
    mongoose.connection.close(); // Close the MongoDB connection when done
  }
};

insertRandomUsers();
