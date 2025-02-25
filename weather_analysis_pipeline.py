from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
import pandas as pd
import requests
import json
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 25)
}

# Create the DAG
dag = DAG(
    'simple_data_pipeline',
    default_args=default_args,
    description='A simple pipeline to extract, process and store data',
    schedule=timedelta(days=1),  # Updated from schedule_interval
    catchup=False
)

# Create data directory if it doesn't exist
def create_data_directory(**kwargs):
    # Create directory in user's home folder
    data_dir = os.path.expanduser("~/weather_data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"Created directory: {data_dir}")
    else:
        print(f"Directory already exists: {data_dir}")
    
    # Create a README file explaining the data
    readme_path = os.path.join(data_dir, "README.txt")
    with open(readme_path, "w") as f:
        f.write("""
        WEATHER DATA DIRECTORY
        ======================
        
        This directory contains weather data collected and processed by the Airflow DAG.
        
        Files:
        - raw_data.json: Raw weather data from the API
        - processed_data.csv: Processed and transformed weather data
        - weather_report.txt: Human-readable summary report
        
        Last updated: {}
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    
    print(f"Created README file at: {readme_path}")
    return data_dir

create_directory_task = PythonOperator(
    task_id='create_data_directory',
    python_callable=create_data_directory,
    dag=dag
)

# Check if API is available
is_api_available = HttpSensor(
    task_id='is_api_available',
    http_conn_id='weather_api',
    endpoint='data/2.5/weather?q=london&appid=your_api_key',
    response_check=lambda response: response.status_code == 200,
    poke_interval=5,
    timeout=20,
    dag=dag
)

# Extract data from API
def get_weather_data(**kwargs):
    print("Starting to fetch weather data...")
    
    # For testing without API key, use mock data
    use_mock_data = True  # Set to False to use actual API
    
    cities = ['New York', 'London', 'Tokyo', 'Sydney', 'Berlin']
    data = []
    
    if use_mock_data:
        # Generate mock data for testing
        print("Using mock data instead of API call")
        for city in cities:
            data.append({
                'city': city,
                'country': 'XX',  # Placeholder country code
                'temperature': round(20 + (cities.index(city) * 2), 1),  # 20-28°C
                'humidity': 60 + (cities.index(city) * 5),  # 60-80%
                'description': 'clear sky' if cities.index(city) % 2 == 0 else 'partly cloudy'
            })
    else:
        # Real API calls
        for city in cities:
            response = requests.get(
                f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid=your_api_key&units=metric'
            )
            if response.status_code == 200:
                result = response.json()
                data.append({
                    'city': city,
                    'country': result['sys']['country'],
                    'temperature': result['main']['temp'],
                    'humidity': result['main']['humidity'],
                    'description': result['weather'][0]['description']
                })
    
    # Save raw data to file
    data_dir = os.path.expanduser("~/weather_data")
    raw_data_path = os.path.join(data_dir, "raw_data.json")
    with open(raw_data_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Saved raw data to: {raw_data_path}")
    print(f"Fetched data for {len(data)} cities")
    
    # Store temporary data for downstream tasks
    kwargs['ti'].xcom_push(key='weather_data', value=data)
    
    return data

extract_weather_data = PythonOperator(
    task_id='extract_weather_data',
    python_callable=get_weather_data,
    dag=dag
)

# Process the data
def process_weather_data(**kwargs):
    print("Starting to process weather data...")
    
    # Get data from previous task
    data = kwargs['ti'].xcom_pull(task_ids='extract_weather_data', key='weather_data')
    
    print(f"Retrieved {len(data)} cities from XCom")
    
    # Create DataFrame and perform transformations
    df = pd.DataFrame(data)
    
    # Convert temperature from C to F
    df['temperature_f'] = df['temperature'] * 9/5 + 32
    
    # Categorize data
    def classify_temp(temp):
        if temp < 10:
            return 'Cold'
        elif temp < 25:
            return 'Moderate'
        else:
            return 'Hot'
    
    df['temp_category'] = df['temperature'].apply(classify_temp)
    
    # Create summary
    summary = {
        'avg_temp': df['temperature'].mean(),
        'max_temp': df['temperature'].max(),
        'min_temp': df['temperature'].min(),
        'cities_count': len(df)
    }
    
    # Save processed data to CSV
    data_dir = os.path.expanduser("~/weather_data")
    processed_data_path = os.path.join(data_dir, "processed_data.csv")
    df.to_csv(processed_data_path, index=False)
    
    print(f"Saved processed data to: {processed_data_path}")
    
    # Push processed data for downstream tasks
    kwargs['ti'].xcom_push(key='processed_data', value=df.to_dict('records'))
    kwargs['ti'].xcom_push(key='summary', value=summary)
    
    return df.to_dict('records')

process_data = PythonOperator(
    task_id='process_weather_data',
    python_callable=process_weather_data,
    dag=dag
)

# Create a report
def create_report(**kwargs):
    print("Starting to generate weather report...")
    
    processed_data = kwargs['ti'].xcom_pull(task_ids='process_weather_data', key='processed_data')
    summary = kwargs['ti'].xcom_pull(task_ids='process_weather_data', key='summary')
    
    report = f"""
    ===== WEATHER REPORT FOR {datetime.now().strftime('%Y-%m-%d')} =====
    
    SUMMARY:
    - Average temperature: {summary['avg_temp']:.1f}°C
    - Highest temperature: {summary['max_temp']:.1f}°C
    - Lowest temperature: {summary['min_temp']:.1f}°C
    - Cities monitored: {summary['cities_count']}
    
    DETAILS BY CITY:
    """
    
    for city_data in processed_data:
        report += f"""
    {city_data['city']} ({city_data['country']}):
    - Temperature: {city_data['temperature']:.1f}°C ({city_data['temp_category']})
    - Humidity: {city_data['humidity']}%
    - Description: {city_data['description']}
        """
    
    # Save report to file in user's home directory
    data_dir = os.path.expanduser("~/weather_data")
    report_path = os.path.join(data_dir, "weather_report.txt")
    
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"Generated weather report at: {report_path}")
    
    return report

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=create_report,
    dag=dag
)

# Print report location
def print_report_location(**kwargs):
    data_dir = os.path.expanduser("~/weather_data")
    report_path = os.path.join(data_dir, "weather_report.txt")
    
    print("\n" + "="*60)
    print(f"REPORT GENERATED SUCCESSFULLY!")
    print(f"View your weather report at: {report_path}")
    print("="*60 + "\n")
    
    # Print a preview of the report
    try:
        with open(report_path, 'r') as f:
            lines = f.readlines()
            preview = "".join(lines[:15])  # First 15 lines
            print("Report Preview:")
            print("-"*30)
            print(preview)
            print("-"*30)
            print(f"... (view full report at {report_path})")
    except Exception as e:
        print(f"Could not read report for preview: {e}")
    
    return "Report generation completed"

print_location = PythonOperator(
    task_id='print_report_location',
    python_callable=print_report_location,
    dag=dag
)

# Define task dependencies
create_directory_task >> is_api_available >> extract_weather_data >> process_data >> generate_report >> print_location

# Add this for direct testing outside of Airflow
if __name__ == "__main__":
    # This allows you to test the script by running it directly
    print("Testing DAG functions directly...")
    
    # Mock task instance for testing
    class MockTI:
        def __init__(self):
            self._data = {}
            
        def xcom_push(self, key, value):
            self._data[key] = value
            return value
                
        def xcom_pull(self, task_ids, key):
            return self._data.get(key)
    
    mock_ti = MockTI()
    
    # Run the entire pipeline
    print("\n===== TESTING ENTIRE PIPELINE =====")
    data_dir = create_data_directory(ti=mock_ti)
    print(f"\nData directory: {data_dir}")
    
    weather_data = get_weather_data(ti=mock_ti)
    print(f"\nExtracted weather data for {len(weather_data)} cities")
    
    processed = process_weather_data(ti=mock_ti)
    print(f"\nProcessed data with {len(processed)} records")
    
    report = create_report(ti=mock_ti)
    print(f"\nGenerated report with {len(report)} characters")
    
    print_report_location(ti=mock_ti)
    
    print("\nTest complete! Check your ~/weather_data directory for results.")
