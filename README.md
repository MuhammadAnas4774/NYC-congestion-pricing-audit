# 🚕 NYC Congestion Pricing Audit

A data analytics toolkit to audit New York City taxi data and simulate the impacts of congestion pricing. This project processes raw TLC trip records to detect anomalies ("ghost trips"), analyze demand elasticity, and visualize revenue potential from the congestion zone (Manhattan south of 60th St).

## 🚀 Features

### 1. Data Pipeline (`pipeline.py`)
- **Automated Ingestion**: Downloads official NYC TLC Trip Record data (Yellow Taxi).
- **Ghost Trip Detection**: Identifies unrealistic trips based on physics constraints:
    - Impossible speeds (>65 MPH)
    - "Teleporting" vehicles (long distance in <1 min)
    - Stationary trips with fares
- **Congestion Zone Analysis**: Flags trips entering the congestion zone and calculates compliance rates.
- **Efficient Processing**: Usies `dask` and `pandas` for handling large datasets.

### 2. Interactive Dashboard (`dashboard.py`)
A Streamlit-based dashboard to visualize the audit results:
- **Hourly Patterns**: Peak demand times and average trip distances.
- **Revenue Analysis**: Total revenue and congestion surcharge collections.
- **Zone Analysis**: Top pickup/dropoff locations and zone entry statistics.
- **Audit Summary**: Compliance metrics and ghost trip statistics.

## 📂 Project Structure

```
├── src/                # Source code for scrapers and utilities
├── outputs/            # Processed data files (Parquet format)
├── dashboard.py        # Streamlit dashboard application
├── pipeline.py         # Main data processing script
├── requirements.txt    # Python dependencies
└── run_dashboard.bat   # Helper script to launch dashboard
```

## 🛠️ Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/MuhammadAnas4774/NYC-congestion-pricing-audit.git
    cd NYC-congestion-pricing-audit
    ```

2.  **Create a virtual environment** (optional but recommended):
    ```bash
    python -m venv venv
    # Windows
    .\venv\Scripts\activate
    # macOS/Linux
    source venv/bin/activate
    ```

3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## 📊 Usage

### Step 1: Run the Data Pipeline
Download and process the latest data. This will generate the `outputs/` directory with clean data.

```bash
python pipeline.py
```

### Step 2: Launch the Dashboard
Start the interactive visualizations.

```bash
streamlit run dashboard.py
```
*Alternatively, double-click `run_dashboard.bat` on Windows.*

## 👤 Author

**MuhammadAnas4774**
- GitHub: [@MuhammadAnas4774](https://github.com/MuhammadAnas4774)
- Email: miananns567@gmail.com
