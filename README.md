# 🗂️ NoSQL Database - Key-Value Storage Engine

A performant, modular key-value store inspired by modern LSM-tree architecture, designed to ensure efficient data persistence, high read/write throughput, and support for probabilistic data structures.

---

## 👥 Authors

- [**Milan Sazdov**](https://github.com/MilanSazdov)  
- [**Vedran Bajić**](https://github.com/vedranbajic4)  
- [**Andrej Dobrić**](https://github.com/loife)  
- [**Ognjen Vujović**](https://github.com/ognjenvujovic04)

---

## 🗃️ Overview

In the era of big data, real-time analytics, and highly scalable applications, traditional relational databases often fall short due to rigid schema constraints, limited scalability, and slower performance under high-concurrency workloads. As a response to these challenges, **NoSQL (Not Only SQL)** databases emerged — a family of database systems purpose-built to manage large volumes of semi-structured, unstructured, and rapidly changing data.

Unlike relational databases, which rely on fixed schemas and SQL for querying, NoSQL systems prioritize **flexibility**, **scalability**, and **performance**, making them well-suited for modern application architectures such as microservices, distributed systems, and cloud-native applications.

### ✅ Why NoSQL?

NoSQL databases are designed to:

- Handle **massive amounts of data** across distributed clusters.
- Support **dynamic and flexible schemas**, adapting to rapidly evolving data models.
- Optimize for **horizontal scalability**, allowing data to be distributed across many machines.
- Enable **fast reads and writes** for real-time use cases like caching, IoT telemetry, analytics, and user personalization.
- Store **diverse types of data**, including documents, key-value pairs, graphs, and wide-column structures — each suited to specific workloads.

These features have made NoSQL databases an essential component in domains such as social networking, content management, recommendation systems, and time-series analysis.

---

## 📌 Core Characteristics of NoSQL Databases

NoSQL systems share several fundamental traits that distinguish them from traditional databases:

- **🔄 Schema Flexibility**  
  NoSQL databases support schema-less or dynamic schemas, meaning data can be inserted without predefined structures. This flexibility simplifies handling evolving datasets, especially in agile or prototyping environments.

- **⚖️ Horizontal Scalability**  
  Many NoSQL systems are designed for distributed environments. They scale "out" rather than "up" — distributing data across multiple nodes for improved load handling and fault tolerance.

- **🚀 High Performance**  
  By relaxing ACID guarantees when appropriate (often favoring eventual consistency), NoSQL databases deliver high throughput and low latency for both reads and writes.

- **🧩 Variety of Data Models**  
  NoSQL is not a single technology — it includes multiple categories:
  - **Key-Value Stores** (e.g., Redis, Riak): Simple and fast, used for caching and session data.
  - **Document Stores** (e.g., MongoDB, CouchDB): Store JSON-like structures; great for content management and user data.
  - **Column-Family Stores** (e.g., Cassandra, HBase): Optimized for analytics over large datasets.
  - **Graph Databases** (e.g., Neo4j, ArangoDB): Ideal for highly connected data like social networks or fraud detection.

- **🛡️ Built-In Fault Tolerance and Replication**  
  Most NoSQL databases support built-in data replication and partitioning to ensure durability and availability even under node failures.

- **🧪 Support for Probabilistic and Analytical Operations**  
  Many modern implementations also integrate lightweight structures like **Bloom Filters**, **Count-Min Sketches**, and **HyperLogLogs** for efficient approximate computations over large data streams.

---

## 🗂️ Types of NoSQL Databases

NoSQL systems are broadly classified into the following categories:

1. **🔑 Key-Value Stores**  
   - Store data as key-value pairs.  
   - Best for simple queries and high-speed lookups.  
   - Examples: Redis, DynamoDB, RocksDB  

2. **📄 Document Stores**  
   - Store semi-structured data as JSON/BSON-like documents.  
   - Ideal for hierarchical and nested data.  
   - Examples: MongoDB, CouchDB  

3. **📊 Column-Family Stores**  
   - Organize data into rows and columns within families.  
   - Used for analytics, wide tables, and time-series data.  
   - Examples: Apache Cassandra, HBase  

4. **🔗 Graph Databases**  
   - Model entities and their relationships as nodes and edges.  
   - Powerful for traversals and complex relationship queries.  
   - Examples: Neo4j, ArangoDB  

Each type offers a different trade-off and is optimized for specific data access patterns and use cases.

---

## 🧠 When to Use NoSQL?

NoSQL is not a one-size-fits-all solution — but it excels in scenarios such as:

- When you need to ingest **huge amounts of data quickly**
- When your application requires **real-time analytics** or **high throughput**
- When your data model is **frequently evolving**
- When you need to scale **horizontally and elastically**
- When your workload is write-heavy and requires **eventual consistency** over strict ACID guarantees

---

## 🎯 Project Goal

The objective of this project is to **design and implement a custom NoSQL key-value database engine** from the ground up — inspired by the architectural principles used in modern storage engines like **LevelDB**, **RocksDB**, and **Cassandra**.

### This project aims to:

- Build an efficient **write path**, consisting of:
  - **Write-Ahead Logging (WAL)** for durability
  - **Memtable** structures (with pluggable backends like `HashMap` or `SkipList`)
  - **Automatic flushing** of Memtables into **SSTable** files on disk

- Build a robust **read path**, enabling:
  - Efficient querying via **Bloom Filters**, **Summary Files**, and **Sparse Indexes**
  - Partial block reading using a **Block Manager**
  - Data validation via **Merkle Trees**

- Support for **probabilistic data structures**, such as:
  - **Bloom Filter** for set membership
  - **Count-Min Sketch** for frequency estimation
  - **HyperLogLog** for cardinality approximation
  - **SimHash** for similarity detection

- Enable **iterators** and **scanning capabilities**, such as:
  - Prefix-based and range-based scans with pagination
  - Cursor-based iteration over large sorted keyspaces

- Incorporate **external configuration**, rate-limiting mechanisms, and modular architecture for easy testing, benchmarking, and extensibility.

By implementing this system from scratch, the project showcases a deep understanding of how modern NoSQL databases are designed, optimized, and integrated into real-world applications.

---

This project demonstrates a **custom-built key-value NoSQL storage engine**, reflecting these core principles by incorporating modular in-memory structures, a durable write-ahead logging system, SSTables, probabilistic filters, and scalable read/write logic.



## 🚀 Features

- **Core Operations**:  
  - `PUT(key, value)`: Inserts or updates a key-value pair  
  - `GET(key)`: Retrieves the value associated with a key  
  - `DELETE(key)`: Marks a key as deleted (tombstone support)  

- **In-Memory Storage (Memtable)**  
  - Supports multiple backends: `HashMap` or `SkipList`  
  - Configurable max size and instance count  
  - Automatically flushed to disk as SSTable on capacity  

- **Write-Ahead Logging (WAL)**  
  - Ensures durability before committing to Memtable  
  - Segment-based logs with CRC checks for data integrity  
  - Fragmentation-aware and supports recovery on startup  

- **SSTables**  
  - Immutable sorted files on disk  
  - Support for:
    - Data, Index, Summary, Filter, Metadata (Merkle Tree)  
    - Sparse indexing and configurable summary granularity  
    - Compressed numeric encodings & optional dictionary compression  
  - Optimized read paths using Bloom Filters and multi-level Indexing  

- **LSM-tree with Compaction**  
  - Size-tiered and leveled compaction strategies  
  - Automatic triggering and level-based SSTable hierarchy  
  - Metadata-driven management of SSTable generations  

- **Caching and Block Management**  
  - LRU Cache support for recently accessed data  
  - Block Manager abstracts disk IO over page-aligned blocks  
  - Configurable block sizes (4KB, 8KB, 16KB)  

- **Probabilistic Structures**  
  - Native support for:  
    - Bloom Filter (membership queries)  
    - Count-Min Sketch (event frequency tracking)  
    - HyperLogLog (cardinality estimation)  
    - SimHash (text fingerprinting and similarity)  
  - Serialized and hidden from standard operations  

- **Scan & Iterate**  
  - `PREFIX_SCAN` and `RANGE_SCAN` with pagination  
  - `PREFIX_ITERATE` and `RANGE_ITERATE` with interactive cursor-based access  

- **Configuration & Rate Limiting**  
  - Fully configurable via external JSON/YAML  
  - Built-in Token Bucket algorithm for access throttling  

---
