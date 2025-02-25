from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import json
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 25)
}

# Create the DAG with daily schedule
dag = DAG(
    'simple_weather_pipeline',
    default_args=default_args,
    description='Simple weather data pipeline for beginners',
    schedule=timedelta(days=1),
    catchup=False
)

# Create data directory
def create_directory():
    data_dir = os.path.expanduser("~/weather_data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"Created directory: {data_dir}")
    return data_dir

# Generate mock weather data (no API call)
def get_weather_data(**kwargs):
    print("Getting weather data...")
    
    cities = ['New York', 'London', 'Tokyo', 'Sydney', 'Berlin']
    data = []
    
    # Generate simple mock data
    for city in cities:
        data.append({
            'city': city,
            'temperature': round(20 + (cities.index(city) * 2), 1),
            'humidity': 60 + (cities.index(city) * 5),
            'description': 'clear sky' if cities.index(city) % 2 == 0 else 'cloudy'
        })
    
    # Save raw data to file
    data_dir = os.path.expanduser("~/weather_data")
    raw_data_path = os.path.join(data_dir, "raw_data.json")
    with open(raw_data_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    # Pass data to next task
    kwargs['ti'].xcom_push(key='weather_data', value=data)
    return data

# Process the weather data
def process_weather_data(**kwargs):
    print("Processing weather data...")
    
    # Get data from previous task
    data = kwargs['ti'].xcom_pull(task_ids='extract_weather_data', key='weather_data')
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Simple transformation: C to F
    df['temperature_f'] = df['temperature'] * 9/5 + 32
    
    # Save processed data
    data_dir = os.path.expanduser("~/weather_data")
    processed_data_path = os.path.join(data_dir, "processed_data.csv")
    df.to_csv(processed_data_path, index=False)
    
    # Pass processed data to next task
    kwargs['ti'].xcom_push(key='processed_data', value=df.to_dict('records'))
    return df.to_dict('records')

# Create a simple report
def create_report(**kwargs):
    print("Creating weather report...")
    
    processed_data = kwargs['ti'].xcom_pull(task_ids='process_weather_data', key='processed_data')
    
    # Calculate average temperature
    temps = [city['temperature'] for city in processed_data]
    avg_temp = sum(temps) / len(temps)
    
    # Create simple report
    report = f"""
    WEATHER REPORT FOR {datetime.now().strftime('%Y-%m-%d')}
    
    Average temperature: {avg_temp:.1f}°C
    Cities monitored: {len(processed_data)}
    
    CITY DETAILS:
    """
    
    for city in processed_data:
        report += f"""
    {city['city']}:
    - Temperature: {city['temperature']}°C ({city['temperature_f']}°F)
    - Humidity: {city['humidity']}%
    - Description: {city['description']}
        """
    
    # Save report
    data_dir = os.path.expanduser("~/weather_data")
    report_path = os.path.join(data_dir, "weather_report.txt")
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"Report saved at: {report_path}")
    return report

# Create tasks
create_directory_task = PythonOperator(
    task_id='create_directory',
    python_callable=create_directory,
    dag=dag
)

extract_weather_data = PythonOperator(
    task_id='extract_weather_data',
    python_callable=get_weather_data,
    dag=dag
)

process_data = PythonOperator(
    task_id='process_weather_data',
    python_callable=process_weather_data,
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=create_report,
    dag=dag
)

# Set up task dependencies (the order of tasks)
create_directory_task >> extract_weather_data >> process_data >> generate_report
