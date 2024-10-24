// seed.js

// Setup the harvest-api user
db = db.getSiblingDB('harvest');
if (db.getUser("harvest-api") == null) {
    db.createUser(
      {
        user: "harvest-api",
        pwd: "default-harvest-password",
        roles: [ { role: "readWrite", db: "harvest" } ]
      }
    );
}

// Setup the admin user
db = db.getSiblingDB('admin');
if (db.getUser("admin") == null) {
    db.createUser(
      {
        user: "admin",
        pwd: "default-harvest-password",
        roles: [ { role: "root", db: "admin" } ]
      }
    );
}

// Connect to the harvest database
db = db.getSiblingDB('harvest');

// Define the documents to be inserted
const documents = [
    {
        name: { family: "Doe", given: "John" },
        age: 30,
        isActive: true,
        address: {
            street: "123 Main St",
            city: "New York",
            zip: "10001"
        },
        tags: ["developer", "javascript", "mongodb"]
    },
    {
        name: { family: "Smith", given: "Jane" },
        age: 25,
        isActive: false,
        address: {
            street: "456 Elm St",
            city: "San Francisco",
            zip: "94101"
        },
        tags: ["designer", "python", "flask"]
    },
    {
        name: { family: "Johnson", given: "Alice" },
        age: 28,
        isActive: true,
        address: {
            street: "789 Oak St",
            city: "Chicago",
            zip: "60601"
        },
        tags: ["manager", "agile", "scrum"]
    },
    {
        name: { family: "Brown", given: "Bob" },
        age: 35,
        isActive: false,
        address: {
            street: "101 Pine St",
            city: "Seattle",
            zip: "98101"
        },
        tags: ["devops", "aws", "docker"]
    },
    {
        name: { family: "Davis", given: "Charlie" },
        age: 40,
        isActive: true,
        address: {
            street: "202 Maple St",
            city: "Austin",
            zip: "73301"
        },
        tags: ["data scientist", "python", "machine learning"]
    },
    {
        name: { family: "Evans", given: "Diana" },
        age: 32,
        isActive: false,
        address: {
            street: "303 Birch St",
            city: "Denver",
            zip: "80201"
        },
        tags: ["product owner", "kanban", "jira"]
    },
    {
        name: { family: "Foster", given: "Eve" },
        age: 29,
        isActive: true,
        address: {
            street: "404 Cedar St",
            city: "Boston",
            zip: "02101"
        },
        tags: ["qa engineer", "selenium", "cypress"]
    },
    {
        name: { family: "Green", given: "Frank" },
        age: 45,
        isActive: false,
        address: {
            street: "505 Spruce St",
            city: "Portland",
            zip: "97201"
        },
        tags: ["security", "pentesting", "networking"]
    },
    {
        name: { family: "Harris", given: "Grace" },
        age: 27,
        isActive: true,
        address: {
            street: "606 Willow St",
            city: "Miami",
            zip: "33101"
        },
        tags: ["frontend", "react", "css"]
    },
    {
        name: { family: "Irving", given: "Henry" },
        age: 38,
        isActive: false,
        address: {
            street: "707 Aspen St",
            city: "Dallas",
            zip: "75201"
        },
        tags: ["backend", "nodejs", "express"]
    }
];

// Drop and re-create the collection
db.users.drop()

// Insert the documents
db.users.insertMany(documents);