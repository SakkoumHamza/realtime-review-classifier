# 🧠 Amazon Kindle Real-Time Review Classifier 🚀

**Data Engineering & AI Project**  
>Real-time sentiment classification of Kindle reviews using a **Kafka + Spark + LSTM (TensorFlow/Keras) + Cassandra** pipeline.


## 🚀 Functionalities

✅ Real-time streaming from **Kafka**  
✅ Scalable, fault-tolerant pipeline using **PySpark Structured Streaming**  
✅ **LSTM** deep learning model with **97.5% accuracy** on unseen data  
✅ Seamless integration with **Apache Cassandra**, a distributed NoSQL database

---
## 📸 Screenshots

### 🔹 Confusion matrix of the model 
![Confusion matrix](screenshots/cm.png)

### 🔹 Kafka producer logs
![Kafka producer logs](screenshots/kafka.png)

### 🔹 Spark consumer logs
![Spark consumer logs](screenshots/spark.png)

### 🔹 Cassandra target table
![Cassandra target table](screenshots/cassandra.png)


---

## 🛠️ Tech Stack

| Component         | Technology                    |
|------------------|-------------------------------|
| Ingestion         | Apache Kafka                 |
| Stream Processing | Apache Spark Structured Streaming |
| AI Model          | LSTM (Keras)                 |
| Database          | Apache Cassandra             |
| Model Format      | `.h5` (Keras)                |

---

## ⚙️ Pipeline Architecture

```text
Kafka (Kindle reviews stream)
        ↓
Spark Structured Streaming
        ↓
Text Preprocessing + LSTM Sentiment Inference
        ↓
Apache Cassandra (target database)

```
## 👨🏻‍💻 Structure de projet

```text
├── data/                    
├── model/
│   ├── model.h5                # Trained LSTM model
│   ├── model_creation.ipynb    # Model creation notebook
│   └── tokenizer.pkl           # Tokenizer for text preprocessing
├── src/
│   ├── spark_consumer.py      
│   ├── kafka_producer.py      
│   └── download_data.py            
├── requirements.txt            # Python dependencies
├── docker-compose.yml             
├── checkpoint.txt
└── README.md                   # You're here!

```

## 🧪 Exemple

```text

Before ->

| reviewID  | reviewerName |  review_text                | reviewTime |
|-----------|--------------|-----------------------------|------------|
| 123abc    | Hamza        | The book was wonderfull!    |  1-18-2013 |

After ->

| reviewID  | reviewerName |  review_text                | sentiment  | reviewTime |
|-----------|--------------|-----------------------------|------------|------------|
| 123abc    |  Hamza       | The book was wonderfull!    | Positive   |  1-18-2013 |

```
 

## 🔧 Setup Instructions 

📦  **Install dependencies**

```bash
pip install -r requirements.txt
```

🐳 **Compose the containers**

```bash
docker-compose up -d
```
🚀 **Launch the kafka producer**

```bash
python src/kafka_producer.py
```

🔄 **Launch the Spark Structured Streaming job**

```bash
python src/spark_consumer.py
```

📊 **Monitor Cassandra**

```bash
docker exec -it cassandra cqlsh
```

```sql
-- Query the reviews table
SELECT * FROM kindle_reviews.reviews;
```
