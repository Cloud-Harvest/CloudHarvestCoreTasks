"""
Test dataset for unit tests.
"""

MONGO_TEST_RECORDS = [
    {
        "name": {"family": "Doe", "given": "John"},
        "age": 30,
        "isActive": True,
        "address": {
            "street": "123 Main St",
            "city": "New York",
            "zip": "10001"
        },
        "tags": ["developer", "javascript", "mongodb"]
    },
    {
        "name": {"family": "Smith", "given": "Jane"},
        "age": 25,
        "isActive": False,
        "address": {
            "street": "456 Elm St",
            "city": "San Francisco",
            "zip": "94101"
        },
        "tags": ["designer", "python", "flask"]
    },
    {
        "name": {"family": "Johnson", "given": "Alice"},
        "age": 28,
        "isActive": True,
        "address": {
            "street": "789 Oak St",
            "city": "Chicago",
            "zip": "60601"
        },
        "tags": ["manager", "agile", "scrum"]
    },
    {
        "name": {"family": "Brown", "given": "Bob"},
        "age": 35,
        "isActive": False,
        "address": {
            "street": "101 Pine St",
            "city": "Seattle",
            "zip": "98101"
        },
        "tags": ["devops", "aws", "docker"]
    },
    {
        "name": {"family": "Davis", "given": "Charlie"},
        "age": 40,
        "isActive": True,
        "address": {
            "street": "202 Maple St",
            "city": "Austin",
            "zip": "73301"
        },
        "tags": ["dataset scientist", "python", "machine learning"]
    },
    {
        "name": {"family": "Evans", "given": "Diana"},
        "age": 32,
        "isActive": False,
        "address": {
            "street": "303 Birch St",
            "city": "Denver",
            "zip": "80201"
        },
        "tags": ["product owner", "kanban", "jira"]
    },
    {
        "name": {"family": "Foster", "given": "Eve"},
        "age": 29,
        "isActive": True,
        "address": {
            "street": "404 Cedar St",
            "city": "Boston",
            "zip": "02101"
        },
        "tags": ["qa engineer", "selenium", "cypress"]
    },
    {
        "name": {"family": "Green", "given": "Frank"},
        "age": 45,
        "isActive": False,
        "address": {
            "street": "505 Spruce St",
            "city": "Portland",
            "zip": "97201"
        },
        "tags": ["security", "pentesting", "networking"]
    },
    {
        "name": {"family": "Harris", "given": "Grace"},
        "age": 27,
        "isActive": True,
        "address": {
            "street": "606 Willow St",
            "city": "Miami",
            "zip": "33101"
        },
        "tags": ["frontend", "react", "css"]
    },
    {
        "name": {"family": "Irving", "given": "Henry"},
        "age": 38,
        "isActive": False,
        "address": {
            "street": "707 Aspen St",
            "city": "Dallas",
            "zip": "75201"
        },
        "tags": ["backend", "nodejs", "express"]
    }
]

REDIS_TEST_RECORDS = [
  {
    "key": "user:1000",
    "value": {
      "name": "John Doe",
      "age": 30,
      "city": "New York"
    }
  },
  {
    "key": "user:1001",
    "value": {
      "name": "Jane Smith",
      "age": 25,
      "city": "San Francisco"
    }
  },
  {
    "key": "user:1002",
    "value": {
      "name": "Alice Johnson",
      "age": 28,
      "city": "Chicago"
    }
  },
  {
    "key": "user:1003",
    "value": {
      "name": "Bob Brown",
      "age": 35,
      "city": "Seattle"
    }
  },
  {
    "key": "user:1004",
    "value": {
      "name": "Charlie Davis",
      "age": 40,
      "city": "Austin"
    }
  },
  {
    "key": "user:1005",
    "value": {
      "name": "Diana Evans",
      "age": 32,
      "city": "Denver"
    }
  },
  {
    "key": "user:1006",
    "value": {
      "name": "Eve Foster",
      "age": 29,
      "city": "Boston"
    }
  },
  {
    "key": "user:1007",
    "value": {
      "name": "Frank Green",
      "age": 45,
      "city": "Portland"
    }
  },
  {
    "key": "user:1008",
    "value": {
      "name": "Grace Harris",
      "age": 27,
      "city": "Miami"
    }
  },
  {
    "key": "user:1009",
    "value": {
      "name": "Henry Irving",
      "age": 38,
      "city": "Dallas"
    }
  }
]