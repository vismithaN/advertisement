# Real-time Advertisement Matching System

A distributed stream processing application built with Apache Samza that matches personalized advertisements to users in real-time based on their profiles and preferences.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [System Components](#system-components)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Data Schema](#data-schema)
- [Building the Project](#building-the-project)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Testing](#testing)
- [Advertisement Matching Algorithm](#advertisement-matching-algorithm)
- [References](#references)

## Overview

This system processes real-time user events and matches them with appropriate advertisements from local businesses (stores). It leverages Apache Samza for stream processing, Kafka for message queuing, and YARN for resource management. The application uses sophisticated matching algorithms based on user attributes such as stress levels, interests, age, mood, and activity levels to provide personalized advertisement recommendations.

## Architecture

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Advertisement Matching System                   │
└─────────────────────────────────────────────────────────────────────────┘

                                ┌──────────────┐
                                │   Kafka      │
                                │   Brokers    │
                                │  (Cluster)   │
                                └──────┬───────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
            ┌───────▼────────┐         │         ┌───────▼────────┐
            │  Input Stream  │         │         │ Output Stream  │
            │    "events"    │         │         │  "ad-stream"   │
            └───────┬────────┘         │         └───────▲────────┘
                    │                  │                 │
                    │                  │                 │
                    │         ┌────────▼────────┐        │
                    └────────►│  AdMatchTask    │────────┘
                              │  (Samza Task)   │
                              └────────┬────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
            ┌───────▼────────┐  ┌──────▼───────┐  ┌──────▼──────┐
            │  RocksDB KV    │  │  RocksDB KV  │  │  Static     │
            │  Store:        │  │  Store:      │  │  Data Files │
            │  "user-info"   │  │  "yelp-info" │  │             │
            │                │  │              │  │ - UserInfo  │
            │  Key: userId   │  │ Key: storeId │  │ - NYCstore  │
            │  (Integer)     │  │ (String)     │  │             │
            └────────────────┘  └──────────────┘  └─────────────┘

            ┌───────────────────────────────────────────────────────┐
            │              YARN Resource Manager                    │
            │                                                       │
            │  ┌──────────────┐  ┌──────────────┐ ┌──────────────┐│
            │  │ Container 1  │  │ Container 2  │ │ Container N  ││
            │  │ (Task 0)     │  │ (Task 1)     │ │ (Task N)     ││
            │  └──────────────┘  └──────────────┘ └──────────────┘│
            └───────────────────────────────────────────────────────┘

            ┌───────────────────────────────────────────────────────┐
            │              HDFS (Hadoop Distributed File System)    │
            │  - Job Package (tar.gz)                               │
            │  - Changelog Topics (State Backup)                    │
            └───────────────────────────────────────────────────────┘
```

### Data Flow

1. **Event Ingestion**: User events are published to Kafka topic `events`
2. **Stream Processing**: AdMatchTask consumes events and processes them
3. **State Access**: Task queries RocksDB stores for user and business data
4. **Matching Logic**: Algorithm matches users with relevant advertisements
5. **Output**: Matched advertisements are published to `ad-stream` topic

## Features

- **Real-time Stream Processing**: Process user events as they arrive
- **Personalized Matching**: Match advertisements based on user profiles including:
  - Stress levels
  - Interests and preferences
  - Age demographics
  - Activity levels
  - Mood states
  - Blood sugar levels
- **Scalable Architecture**: Distributed processing on YARN cluster
- **Fault Tolerance**: State management with RocksDB and Kafka changelog topics
- **Category-based Tagging**: Businesses categorized into five main groups (exact code values shown):
  - `lowCalories`: `seafood`, `vegetarian`, `vegan`, `sushi`
  - `energyProviders`: `bakeries`, `ramen`, `donuts`, `burgers`, `bagels`, `pizza`, `sandwiches`, `icecream`, `desserts`, `bbq`, `dimsum`, `steak`
  - `willingTour`: `parks`, `museums`, `newamerican`, `landmarks`
  - `stressRelease`: `coffee`, `bars`, `wine_bars`, `cocktailbars`, `lounges`
  - `happyChoice`: `italian`, `thai`, `cuban`, `japanese`, `mideastern`, `cajun`, `tapas`, `breakfast_brunch`, `korean`, `mediterranean`, `vietnamese`, `indpak`, `southern`, `latin`, `greek`, `mexican`, `asianfusion`, `spanish`, `chinese`

## System Components

### Core Classes

- **`AdMatchTask.java`**: Main stream processing task implementing `StreamTask` and `InitableTask`
  - Processes incoming events
  - Manages KV stores
  - Implements advertisement matching logic

- **`AdMatchConfig.java`**: Configuration class
  - Defines Kafka system streams
  - Provides utility methods for reading static data files

- **`AdMatchTaskApplication.java`**: Application descriptor
  - Configures Samza application
  - Sets up Kafka system descriptors
  - Defines input/output stream descriptors

### Key-Value Stores

1. **user-info**: Stores user profile data
   - Key: Integer (userId)
   - Value: Map containing user attributes (stress, mood, age, interest, etc.)

2. **yelp-info**: Stores business/store data
   - Key: String (storeId)
   - Value: Map containing store details (name, category, rating, price, location)

## Prerequisites

- **Java**: JDK 1.8 or higher
- **Maven**: 3.0.0 or higher
- **Apache Samza**: 1.2.0
- **Apache Kafka**: 0.10.1.1
- **Hadoop**: 2.8.3
- **YARN**: Configured cluster with running ResourceManager
- **Zookeeper**: For Kafka coordination

## Project Structure

```
advertisement/
├── pom.xml                          # Maven build configuration
├── runner.sh                        # Deployment script
├── references                       # Reference citations
├── submitter_task3                  # Submission binary
├── src/
│   ├── main/
│   │   ├── java/com/cloudcomputing/samza/nycabs/
│   │   │   ├── AdMatchTask.java              # Main task logic
│   │   │   ├── AdMatchConfig.java            # Configuration
│   │   │   └── application/
│   │   │       └── AdMatchTaskApplication.java
│   │   ├── config/
│   │   │   └── ad-match.properties           # Job configuration
│   │   ├── resources/
│   │   │   ├── UserInfoData.json             # User profiles (3.6MB)
│   │   │   ├── NYCstore.json                 # Business data (224KB)
│   │   │   └── log4j.xml                     # Logging configuration
│   │   └── assembly/
│   │       └── src.xml                       # Assembly descriptor
│   └── test/
│       ├── java/com/cloudcomputing/samza/nycabs/
│       │   ├── TestAdMatchTask.java          # Unit tests
│       │   └── TestUtils.java                # Test utilities
│       └── resources/
│           ├── UserInfoData.json             # Test data
│           └── NYCstore.json                 # Test data
└── README.md                        # This file
```

## Data Schema

### User Profile Schema
```json
{
  "userId": 0,
  "type": "user",
  "stress": 7,           // Stress level (0-10)
  "travel_count": 13,    // Number of trips
  "mood": 3,             // Mood level (0-10)
  "gender": "M",         // M/F
  "age": 20,             // Age in years
  "active": 3,           // Activity level (0-10)
  "interest": "sandwiches", // Primary interest category
  "device": "iPhone 5",  // Device type
  "blood_sugar": 1       // Blood sugar level (0-10)
}
```

### Business/Store Schema
```json
{
  "storeId": "H4jJ7XB3CetIr1pg56CczQ",
  "type": "yelp",
  "name": "Levain Bakery",
  "review_count": 6974,
  "categories": "bakeries",
  "rating": 4.5,
  "price": "$$",
  "latitude": 40.7799404643263,
  "longitude": -73.980282552649,
  "blockId": 45
}
```

### Event Schema
Events consumed from the `events` Kafka topic typically contain user activity data. Example structure:
```json
{
  "userId": 0,
  "blockId": 45,
  "latitude": 40.7799,
  "longitude": -73.9802,
  "timestamp": 1234567890,
  "eventType": "location_update"
}
```

The event structure should include:
- `userId`: User identifier for matching with user profile
- `blockId`: Geographic block identifier for location-based matching
- `latitude`/`longitude`: GPS coordinates for proximity calculations
- `timestamp`: Event timestamp
- `eventType`: Type of event (location_update, interaction, etc.)

### Advertisement Output Schema
Matched advertisements published to `ad-stream` Kafka topic containing:
- `userId`: Target user ID
- `name`: Business name
- Additional store details from matching logic

## Building the Project

### 1. Clean and Build
```bash
mvn clean package
```

This will:
- Compile Java source files
- Run unit tests
- Create JAR file: `target/nycabs-0.0.1.jar`
- Generate distribution package: `target/nycabs-0.0.1-dist.tar.gz`

### 2. Build Output
- **JAR**: `target/nycabs-0.0.1.jar`
- **Distribution Package**: `target/nycabs-0.0.1-dist.tar.gz`

## Configuration

### Update `src/main/config/ad-match.properties`

Before deployment, update the following properties:

```properties
# Update with master node DNS
yarn.package.path=hdfs://ip-1-2-3-4.ec2.internal:8020/nycabs-0.0.1-dist.tar.gz

# Update with master node DNS
systems.kafka.consumer.zookeeper.connect=ip-1-2-3-4.ec2.internal:2181/

# Update with all cluster node DNS (comma-separated)
systems.kafka.producer.bootstrap.servers=ip-1-2-3-4.ec2.internal:9092,ip-5-6-7-8.ec2.internal:9092
```

### Key Configuration Parameters

- **Job Configuration**
  - `job.name`: ad-match
  - `job.factory.class`: YarnJobFactory for YARN deployment

- **Kafka Streams**
  - Input: `events` (offset: oldest, reset enabled)
  - Output: `ad-stream`

- **State Stores**
  - `user-info`: RocksDB store with integer keys
  - `yelp-info`: RocksDB store with string keys
  - Replication factor: 3 (for fault tolerance)

## Deployment

### Automatic Deployment with runner.sh

```bash
chmod +x runner.sh
./runner.sh
```

The script performs:
1. Creates deployment directory: `deploy/samza/`
2. Builds project with Maven
3. Extracts distribution package
4. Copies package to HDFS
5. Launches job on YARN cluster

### Manual Deployment Steps

```bash
# 1. Build the project
mvn clean package

# 2. Extract distribution
mkdir -p deploy/samza
tar -xvf target/nycabs-0.0.1-dist.tar.gz -C deploy/samza/

# 3. Upload to HDFS
hadoop fs -copyFromLocal -f target/nycabs-0.0.1-dist.tar.gz /

# 4. Run the application
deploy/samza/bin/run-app.sh \
  --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
  --config-path="file://$PWD/deploy/samza/config/ad-match.properties"
```

### Verify Deployment

```bash
# Check YARN applications
yarn application -list

# View application logs
yarn logs -applicationId <application_id>

# Monitor Kafka topics
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ad-stream
```

## Testing

### Run Unit Tests

```bash
mvn test
```

### Test Classes

- **`TestAdMatchTask.java`**: Tests the advertisement matching logic
  - Tests base score matching
  - Tests interest-based matching
  - Tests affordability matching
  - Tests status update handling
  - Tests age-based matching

### Test Framework
- Uses Samza's `TestRunner` framework
- In-memory descriptors for streams
- RocksDB for state management
- Duration-based test execution

### Expected Test Results
The test suite validates 5 different matching scenarios:
1. Base score test (userId: 0, Cloud Bakery)
2. Interest test (userId: 1, Cloud Ramen)
3. Afford test (userId: 2, Luxury Cloud Bakery)
4. Update status test (userId: 3, Cloud Cafe)
5. Age test (userId: 4, Cloud Bakery II)

## Advertisement Matching Algorithm

### Matching Criteria

The system matches advertisements based on several factors:

1. **Category Matching**: Aligns user interests with business categories
   - Users interested in "bakeries" → energyProviders tag
   - Users interested in "parks" → willingTour tag
   - Users interested in "coffee" → stressRelease tag

2. **Profile-based Scoring**:
   - **Stress Level**: High stress users matched with stress-relief businesses
   - **Mood**: Mood influences category preferences
   - **Activity Level**: Active users get different recommendations
   - **Age Demographics**: Age-appropriate suggestions
   - **Blood Sugar**: Health-conscious matching

3. **Location-based Filtering**: Uses geographic coordinates (latitude/longitude)
   - Proximity calculation using Haversine formula
   - Distance-based relevance scoring

4. **Rating and Price Considerations**:
   - Business rating (0-5 stars)
   - Price range ($, $$, $$$, $$$$)
   - Review count for popularity

### Category Tags

The following category tags are used in the code for business classification (exact code values as defined in `AdMatchTask.java`):

- **lowCalories**: `seafood`, `vegetarian`, `vegan`, `sushi`
- **energyProviders**: `bakeries`, `ramen`, `donuts`, `burgers`, `bagels`, `pizza`, `sandwiches`, `icecream`, `desserts`, `bbq`, `dimsum`, `steak`
- **willingTour**: `parks`, `museums`, `newamerican`, `landmarks`
- **stressRelease**: `coffee`, `bars`, `wine_bars`, `cocktailbars`, `lounges`
- **happyChoice**: `italian`, `thai`, `cuban`, `japanese`, `mideastern`, `cajun`, `tapas`, `breakfast_brunch`, `korean`, `mediterranean`, `vietnamese`, `indpak`, `southern`, `latin`, `greek`, `mexican`, `asianfusion`, `spanish`, `chinese`

## References

The `references` file contains citations for external resources and collaborations used in this project. Please maintain proper citations to avoid academic integrity issues.

## Contributing

When working on this project:
1. Limit collaboration to block diagrams and architectural discussions
2. Do not share or view code from other students
3. Properly cite all external resources and code snippets
4. Use JSON validators for configuration files
5. Follow the course and university policies on academic integrity

## License

This project is part of a cloud computing coursework assignment.

## Support

For issues or questions:
1. Review Apache Samza documentation: https://samza.apache.org/
2. Check Kafka documentation: https://kafka.apache.org/
3. Consult Hadoop YARN documentation: https://hadoop.apache.org/

---

**Note**: This is an educational project demonstrating real-time stream processing with Apache Samza. The matching algorithm is a simplified example and should be enhanced with more sophisticated machine learning models for production use.
