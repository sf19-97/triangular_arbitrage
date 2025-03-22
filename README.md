# CurrencyMatrixRealtime

CurrencyMatrixRealtime is a real-time system for fetching live exchange rates from OANDA’s v20 streaming API and constructing an exchange rate matrix in log-space. This matrix is used to derive implied cross rates and detect potential arbitrage opportunities among major currencies.

## Overview

In an arbitrage-free market, the exchange rate between two currencies can be represented as the difference in their intrinsic log-values. If each currency \(i\) has an intrinsic potential \(v_i\), then the exchange rate from currency \(i\) to \(j\) is:
\[
\ln R_{ij} = v_i - v_j
\]
CurrencyMatrixRealtime leverages this idea by:
- Streaming tick-level data from OANDA.
- Converting received bid and ask prices into mid-prices and then into natural logarithms.
- Building an \(8 \times 8\) log exchange rate matrix for the major currencies.
- (Optionally) Updating a consensus view (potentials) to detect arbitrage opportunities.

## Folder Structure

triangular_arbitrage/ │ ├── .vscode/ # VS Code-specific settings (optional) ├── config/ # Application configuration files (e.g., settings_template.json) ├── docker/ # Dockerfile and docker-compose.yml for containerization ├── docs/ # Additional documentation (if needed) ├── src/ # Source code files │ ├── fetch_oanda.py # Module to fetch live rates from OANDA │ ├── matrix_builder.py # Module to build the log exchange rate matrix │ └── stream_matrix.py # Main script for streaming and updating the matrix ├── CurrencyMatrixRealtime.code-workspace # VS Code workspace file ├── README.md # This documentation file ├── requirements.txt # List of Python dependencies └── (other files like .gitignore, LICENSE, etc.)

markdown
Copy

## Setup and Requirements

### Prerequisites
- **Python 3.9+** (or your preferred version)
- An OANDA account (Practice or Live) with access to the v20 API.
- Your OANDA credentials:
  - `OANDA_ACCOUNT_ID`
  - `OANDA_API_KEY`
- (Optional) Docker and docker-compose for containerization.
- (Optional) Kafka and Zookeeper if you plan to integrate additional streaming functionality.

### Python Dependencies
All required packages are listed in `requirements.txt`. For example, it may include:
requests pandas numpy schedule

go
Copy
Install them via:
```bash
pip install -r requirements.txt
Environment Variables
Set the following environment variables for OANDA API authentication:

OANDA_ACCOUNT_ID: Your OANDA account ID.
OANDA_API_KEY: Your OANDA API access token.
On Windows, you can set them in your Command Prompt:

bash
Copy
set OANDA_ACCOUNT_ID=your_account_id
set OANDA_API_KEY=your_api_key
Or configure them in your VS Code launch configuration.

Running the Application
Local Development
Open your project folder (F:\triangular_arbitrage) in VS Code.
Ensure your environment variables are set.
Open a terminal in VS Code and run the streaming script:
bash
Copy
python src/stream_matrix.py
The script will connect to OANDA’s streaming endpoint, receive tick-level data, update the live rates, and rebuild the log exchange rate matrix in real time. Updated matrix output will be printed in the terminal.
Docker Deployment
If you wish to containerize your application:

Navigate to the docker/ folder.
Build the Docker image:
bash
Copy
docker build -t currency-matrix .
Run the container, ensuring you pass the environment variables:
bash
Copy
docker run -d -e OANDA_API_KEY=your_api_key -e OANDA_ACCOUNT_ID=your_account_id -p 8000:8000 currency-matrix
(Adjust the exposed port as necessary if you later serve an API.)
Real-Time Streaming and Matrix Update
The real-time module uses OANDA's streaming pricing endpoint to receive live tick data and update the exchange rate matrix. Here's how it works:

Streaming Endpoint:
The script in src/stream_matrix.py connects to:

bash
Copy
https://stream-fxpractice.oanda.com/v3/accounts/{account_id}/pricing/stream?instruments=EUR_USD,USD_JPY,GBP_USD,USD_CHF,CAD_USD,AUD_USD,NZD_USD
Replace {account_id} with your OANDA account ID.

Processing Tick Data:
Each incoming tick is processed to extract the bid and ask prices, compute a mid-price, and update a global dictionary (live_rates).

Matrix Construction:
Once live rates for all instruments are available, the function build_exchange_matrix() from src/matrix_builder.py is used to create an 8×8 log exchange rate matrix using USD as the numeraire.

Output:
The updated matrix is printed to the terminal, and can be further integrated into an arbitrage detection pipeline.

Docker Configuration Files
Dockerfile (in docker/Dockerfile)
dockerfile
Copy
FROM python:3.9-slim
WORKDIR /app

# Copy and install dependencies
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY ../src ./src

# Expose a port if serving an API
EXPOSE 8000

# Run the streaming script
CMD ["python", "src/stream_matrix.py"]
docker-compose.yml (optional, in docker/docker-compose.yml)
yaml
Copy
version: '3.8'
services:
  arbitrage-service:
    build: .
    container_name: currency_matrix
    environment:
      - OANDA_API_KEY=${OANDA_API_KEY}
      - OANDA_ACCOUNT_ID=${OANDA_ACCOUNT_ID}
    ports:
      - "8000:8000"
Additional Notes
Kafka Integration:
If you have an existing Kafka setup (e.g., in C:\kafka-docker), you can either keep it separate or integrate its services into your docker-compose.yml for a unified deployment.
Error Handling and Logging:
The streaming script includes basic error handling and reconnection logic. For production use, consider enhancing logging and error recovery.
VS Code Workspace:
Use the provided workspace file (CurrencyMatrixRealtime.code-workspace) to manage this project in VS Code.
Version Control:
Use a .gitignore file to exclude files like __pycache__, .env, and other non-essential artifacts.
Contact and Support
For questions or issues, please open an issue in the project’s repository or contact the project maintainer.

yaml
Copy

---

This README.md contains everything you need to understand, set up, and run the CurrencyMatrixReal