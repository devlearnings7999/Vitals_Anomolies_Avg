# Vitals Anomaly Averaging

This application consumes real-time health vitals data from a Kafka topic, calculates the average of each vital parameter every 10 seconds, and publishes the averaged vitals to another Kafka topic.

## Prerequisites

-   Python 3.9+
-   Docker (optional, for containerization)
-   Kafka cluster

## Setup

1.  **Clone the repository**:
    ```bash
    git clone <repository_url>
    cd Vitals_Anomolies_Avg
    ```

2.  **Configure the application**:
    -   Create a `.env` file in the `Vitals_Anomolies_Avg` directory.
    -   Add the following environment variables to the `.env` file:
        ```
        KAFKA_BROKER=<your_broker_address>
        INPUT_TOPIC=vitals-ml-test1
        OUTPUT_TOPIC=vitals_anomalies_avg
        SECURITY_PROTOCOL=SASL_PLAINTEXT
        SASL_MECHANISM=SCRAM-SHA-512
        SASL_USERNAME=<your_username>
        SASL_PASSWORD=<your_password>
        ```
        Replace `<your_broker_address>`, `<your_username>`, and `<your_password>` with your Kafka broker address and credentials.

3.  **Install dependencies**:
    ```bash
    pip install python-dotenv kafka-python
    ```

## Running the application

1.  **Start the application**:
    ```bash
    python main.py
    ```

## Running with Docker (optional)

1.  **Build the Docker image**:
    ```bash
    docker build -t vitals-averager .
    ```

2.  **Run the Docker container**:
    ```bash
    docker run -d --name vitals-averager --env-file .env vitals-averager
    ```

## Notes

-   Ensure that the Kafka broker is running and accessible.
-   The application consumes messages from the `vitals-ml-test1` topic and publishes averaged vitals to the `vitals_anomalies_avg` topic.
-   The averaging interval is set to 10 seconds.
-   The application handles `KeyboardInterrupt` to stop the consumer gracefully.
