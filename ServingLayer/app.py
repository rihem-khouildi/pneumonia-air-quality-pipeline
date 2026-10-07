from flask import Flask, render_template, jsonify, Response
from cassandra.cluster import Cluster
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from plotly.utils import PlotlyJSONEncoder
from datetime import datetime
import time

app = Flask(__name__)

# -------------------------
# Cassandra Configuration
# -------------------------
CASSANDRA_HOSTS = ['127.0.0.1']
KEYSPACE = 'iot_keyspace'
IOT_DATA_TABLE = 'iot_data'

def get_cassandra_session():
    """Create and return a Cassandra session with error handling"""
    try:
        cluster = Cluster(CASSANDRA_HOSTS, connect_timeout=10)
        session = cluster.connect(KEYSPACE)
        return session, cluster
    except Exception as e:
        print(f"❌ Cassandra Connection Error: {e}")
        return None, None

def fetch_iot_data(limit=1000):
    """Fetch real-time patient data from iot_data table"""
    session, cluster = get_cassandra_session()
    
    if not session:
        return pd.DataFrame()
    
    try:
        query = f"""
        SELECT patient_id, body_temperature, chest_pain, city, cough, country,
               heart_rate, label, no2, oxygen_saturation, pm10, pm25,
               respiratory_rate, shortness_of_breath, who_region, year
        FROM {IOT_DATA_TABLE}
        LIMIT {limit};
        """
        rows = session.execute(query)
        df = pd.DataFrame(list(rows))
        
        if not df.empty:
            # Fill NaN values
            numeric_columns = ['body_temperature', 'heart_rate', 'oxygen_saturation',
                             'respiratory_rate', 'pm25', 'pm10', 'no2']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2)
            
            # Fill binary columns
            binary_columns = ['chest_pain', 'cough', 'shortness_of_breath']
            for col in binary_columns:
                if col in df.columns:
                    df[col] = df[col].fillna(0).astype(int)
        
        return df
    
    except Exception as e:
        print(f"❌ Error fetching IoT data: {e}")
        return pd.DataFrame()
    
    finally:
        if cluster:
            cluster.shutdown()

def create_realtime_visualizations(df_iot):
    """Create comprehensive visualizations for real-time data"""
    
    if df_iot.empty:
        return None
    
    visualizations = {}
    
    # -------------------------
    # Statistics
    # -------------------------
    stats = {
        'total_patients': int(len(df_iot)),
        'pneumonia_cases': int(len(df_iot[df_iot['label'] == 'PNEUMONIA'])),
        'normal_cases': int(len(df_iot[df_iot['label'] == 'NORMAL'])),
        'avg_spo2': float(df_iot['oxygen_saturation'].mean().round(2)),
        'cities_monitored': int(df_iot['city'].nunique()),
        'timestamp': datetime.now().strftime('%H:%M:%S')
    }
    
    if stats['total_patients'] > 0:
        stats['pneumonia_rate'] = round((stats['pneumonia_cases'] / stats['total_patients']) * 100, 2)
    else:
        stats['pneumonia_rate'] = 0
    
    visualizations['stats'] = stats
    
    # -------------------------
    # 1. Real-time Patient Status Distribution
    # -------------------------
    label_counts = df_iot['label'].value_counts()
    
    fig_status = go.Figure(data=[
        go.Pie(
            labels=label_counts.index,
            values=label_counts.values,
            hole=0.5,
            marker=dict(colors=['#48bb78', '#f56565']),
            textinfo='label+percent+value',
            textfont=dict(size=14)
        )
    ])
    
    fig_status.update_layout(
        title="🩺 Real-Time Patient Status Distribution",
        height=400,
        annotations=[dict(text='Status', x=0.5, y=0.5, font_size=20, showarrow=False)]
    )
    
    visualizations['status_dist'] = json.dumps(fig_status, cls=PlotlyJSONEncoder)
    
    # -------------------------
    # 2. Live Health Metrics Dashboard
    # -------------------------
    fig_health = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Heart Rate Distribution", "Oxygen Saturation (SpO2)",
                      "Body Temperature", "Respiratory Rate"),
        specs=[[{"type": "histogram"}, {"type": "histogram"}],
               [{"type": "histogram"}, {"type": "histogram"}]]
    )
    
    # Heart Rate
    fig_health.add_trace(
        go.Histogram(
            x=df_iot['heart_rate'],
            name='Heart Rate',
            marker_color='crimson',
            nbinsx=30
        ),
        row=1, col=1
    )
    
    # Oxygen Saturation
    fig_health.add_trace(
        go.Histogram(
            x=df_iot['oxygen_saturation'],
            name='SpO2',
            marker_color='lightblue',
            nbinsx=30
        ),
        row=1, col=2
    )
    
    # Body Temperature
    fig_health.add_trace(
        go.Histogram(
            x=df_iot['body_temperature'],
            name='Temperature',
            marker_color='orange',
            nbinsx=30
        ),
        row=2, col=1
    )
    
    # Respiratory Rate
    fig_health.add_trace(
        go.Histogram(
            x=df_iot['respiratory_rate'],
            name='Resp. Rate',
            marker_color='purple',
            nbinsx=30
        ),
        row=2, col=2
    )
    
    fig_health.update_layout(
        title_text="📊 Live Health Metrics Dashboard",
        showlegend=False,
        height=600
    )
    
    visualizations['health_metrics'] = json.dumps(fig_health, cls=PlotlyJSONEncoder)
    
    # -------------------------
    # 3. Geographic Distribution Map - FIXED
    # -------------------------
    # Group data by country
    country_data = df_iot.groupby('country').agg({
        'patient_id': 'count',
        'oxygen_saturation': 'mean'
    }).reset_index()
    country_data.columns = ['country', 'total_patients', 'avg_spo2']
    
    # Calculate pneumonia percentage
    pneumonia_by_country = df_iot[df_iot['label'] == 'PNEUMONIA'].groupby('country').size()
    country_data['pneumonia_count'] = country_data['country'].map(pneumonia_by_country).fillna(0)
    country_data['pneumonia_pct'] = (country_data['pneumonia_count'] / country_data['total_patients'] * 100).round(2)
    
    # Round values for better display
    country_data['avg_spo2'] = country_data['avg_spo2'].round(2)
    
    print(f"📊 Country data prepared: {len(country_data)} countries")
    print(country_data.head())
    
    # Create choropleth map (better for country-level data)
    fig_map = go.Figure(data=go.Choropleth(
        locations=country_data['country'],
        locationmode='ISO-3',
        z=country_data['pneumonia_pct'],
        text=country_data['country'],
        colorscale='Reds',
        autocolorscale=False,
        reversescale=False,
        marker_line_color='darkgray',
        marker_line_width=0.5,
        colorbar_title="Pneumonia %",
        hovertemplate='<b>%{text}</b><br>' +
                      'Patients: %{customdata[0]}<br>' +
                      'Pneumonia: %{z:.2f}%<br>' +
                      'Avg SpO2: %{customdata[1]:.2f}%<br>' +
                      '<extra></extra>',
        customdata=country_data[['total_patients', 'avg_spo2']].values
    ))
    
    fig_map.update_layout(
        title_text='🌍 Global Patient Distribution by Country',
        geo=dict(
            showframe=True,
            showcoastlines=True,
            projection_type='natural earth',
            showcountries=True,
            countrycolor="lightgray",
            coastlinecolor="gray",
            showland=True,
            landcolor="rgb(243, 243, 243)",
            showocean=True,
            oceancolor="rgb(204, 229, 255)",
        ),
        height=600
    )
    
    visualizations['geo_map'] = json.dumps(fig_map, cls=PlotlyJSONEncoder)
    
    # -------------------------
    # 4. Symptoms Analysis
    # -------------------------
    symptoms_data = {
        'Cough': int(df_iot['cough'].sum()),
        'Chest Pain': int(df_iot['chest_pain'].sum()),
        'Shortness of Breath': int(df_iot['shortness_of_breath'].sum())
    }
    
    fig_symptoms = go.Figure(data=[
        go.Bar(
            x=list(symptoms_data.keys()),
            y=list(symptoms_data.values()),
            marker_color=['#ff6b6b', '#4ecdc4', '#45b7d1'],
            text=list(symptoms_data.values()),
            textposition='auto',
        )
    ])
    
    fig_symptoms.update_layout(
        title="🤒 Symptom Prevalence",
        xaxis_title="Symptom",
        yaxis_title="Number of Cases",
        height=400
    )
    
    visualizations['symptoms'] = json.dumps(fig_symptoms, cls=PlotlyJSONEncoder)
    
    # -------------------------
    # 5. Air Quality by Region
    # -------------------------
    regional_air = df_iot.groupby('who_region').agg({
        'pm25': 'mean',
        'pm10': 'mean',
        'no2': 'mean'
    }).reset_index()
    
    fig_air = go.Figure()
    
    fig_air.add_trace(go.Bar(
        name='PM2.5',
        x=regional_air['who_region'],
        y=regional_air['pm25'],
        marker_color='indianred'
    ))
    
    fig_air.add_trace(go.Bar(
        name='PM10',
        x=regional_air['who_region'],
        y=regional_air['pm10'],
        marker_color='lightsalmon'
    ))
    
    fig_air.add_trace(go.Bar(
        name='NO2',
        x=regional_air['who_region'],
        y=regional_air['no2'],
        marker_color='lightblue'
    ))
    
    fig_air.update_layout(
        title="🌫️ Average Air Quality by WHO Region",
        xaxis_title="WHO Region",
        yaxis_title="Concentration (μg/m³)",
        barmode='group',
        height=500,
        xaxis={'tickangle': -45}
    )
    
    visualizations['air_quality'] = json.dumps(fig_air, cls=PlotlyJSONEncoder)
    
    # -------------------------
    # 6. Top 15 Cities
    # -------------------------
    city_counts = df_iot.groupby(['city', 'country']).size().reset_index(name='patient_count')
    city_counts = city_counts.nlargest(15, 'patient_count')
    city_counts['city_country'] = city_counts['city'] + ', ' + city_counts['country']
    
    fig_cities = go.Figure(data=[
        go.Bar(
            x=city_counts['patient_count'],
            y=city_counts['city_country'],
            orientation='h',
            marker=dict(
                color=city_counts['patient_count'],
                colorscale='Viridis'
            ),
            text=city_counts['patient_count'],
            textposition='auto'
        )
    ])
    
    fig_cities.update_layout(
        title="🏙️ Top 15 Cities by Patient Count",
        xaxis_title="Number of Patients",
        yaxis_title="City",
        height=600,
        showlegend=False
    )
    
    visualizations['top_cities'] = json.dumps(fig_cities, cls=PlotlyJSONEncoder)
    
    # -------------------------
    # 7. Health Status by Region
    # -------------------------
    region_status = pd.crosstab(df_iot['who_region'], df_iot['label'])
    
    fig_region_status = go.Figure()
    
    for status in region_status.columns:
        color = '#48bb78' if status == 'NORMAL' else '#f56565'
        fig_region_status.add_trace(go.Bar(
            name=status,
            x=region_status.index,
            y=region_status[status],
            marker_color=color
        ))
    
    fig_region_status.update_layout(
        title="🌐 Patient Status by WHO Region",
        xaxis_title="WHO Region",
        yaxis_title="Number of Patients",
        barmode='stack',
        height=500,
        xaxis={'tickangle': -45}
    )
    
    visualizations['region_status'] = json.dumps(fig_region_status, cls=PlotlyJSONEncoder)
    
    return visualizations

# -------------------------
# Flask Routes
# -------------------------
@app.route('/')
def index():
    """Main real-time dashboard"""
    try:
        df_iot = fetch_iot_data(limit=1000)
        
        if df_iot.empty:
            return """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Error - No Data</title>
                <style>
                    body {
                        font-family: Arial, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        height: 100vh;
                        margin: 0;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    }
                    .error-box {
                        background: white;
                        padding: 40px;
                        border-radius: 20px;
                        text-align: center;
                        box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                        max-width: 500px;
                    }
                    h1 { color: #f56565; margin-bottom: 20px; }
                    p { color: #666; line-height: 1.6; }
                    .btn {
                        display: inline-block;
                        margin-top: 20px;
                        padding: 12px 30px;
                        background: #667eea;
                        color: white;
                        text-decoration: none;
                        border-radius: 25px;
                        font-weight: 600;
                    }
                    .btn:hover { background: #764ba2; }
                </style>
            </head>
            <body>
                <div class="error-box">
                    <h1>⚠️ No Data Available</h1>
                    <p>Cannot connect to Cassandra or no data found in <code>iot_data</code> table.</p>
                    <p><strong>Please check:</strong></p>
                    <ul style="text-align: left; color: #666;">
                        <li>Cassandra is running on localhost:9042</li>
                        <li>Keyspace 'iot_keyspace' exists</li>
                        <li>Table 'iot_data' has data</li>
                    </ul>
                    <a href="/" class="btn">🔄 Retry</a>
                </div>
            </body>
            </html>
            """, 500
        
        visualizations = create_realtime_visualizations(df_iot)
        
        if not visualizations:
            return """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Error - Visualization</title>
                <style>
                    body {
                        font-family: Arial, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        height: 100vh;
                        margin: 0;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    }
                    .error-box {
                        background: white;
                        padding: 40px;
                        border-radius: 20px;
                        text-align: center;
                        box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                    }
                    h1 { color: #f56565; }
                    .btn {
                        display: inline-block;
                        margin-top: 20px;
                        padding: 12px 30px;
                        background: #667eea;
                        color: white;
                        text-decoration: none;
                        border-radius: 25px;
                        font-weight: 600;
                    }
                </style>
            </head>
            <body>
                <div class="error-box">
                    <h1>⚠️ Visualization Error</h1>
                    <p>Could not create visualizations from the data.</p>
                    <a href="/" class="btn">🔄 Retry</a>
                </div>
            </body>
            </html>
            """, 500
        
        return render_template('dashboard.html', **visualizations)
    
    except Exception as e:
        print(f"❌ Error in index route: {e}")
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Server Error</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                }}
                .error-box {{
                    background: white;
                    padding: 40px;
                    border-radius: 20px;
                    text-align: center;
                    box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                    max-width: 600px;
                }}
                h1 {{ color: #f56565; }}
                code {{
                    background: #f5f5f5;
                    padding: 10px;
                    display: block;
                    margin: 20px 0;
                    border-radius: 5px;
                    color: #e83e8c;
                }}
                .btn {{
                    display: inline-block;
                    margin-top: 20px;
                    padding: 12px 30px;
                    background: #667eea;
                    color: white;
                    text-decoration: none;
                    border-radius: 25px;
                    font-weight: 600;
                }}
            </style>
        </head>
        <body>
            <div class="error-box">
                <h1>🔥 Server Error</h1>
                <p>An unexpected error occurred:</p>
                <code>{str(e)}</code>
                <a href="/" class="btn">🔄 Retry</a>
            </div>
        </body>
        </html>
        """, 500

@app.route('/api/realtime-data')
def api_realtime_data():
    """API endpoint for real-time data (for auto-refresh)"""
    try:
        df_iot = fetch_iot_data(limit=100)
        
        if df_iot.empty:
            return jsonify({'error': 'No data available'}), 500
        
        # Return latest stats
        stats = {
            'total_patients': int(len(df_iot)),
            'pneumonia_cases': int(len(df_iot[df_iot['label'] == 'PNEUMONIA'])),
            'normal_cases': int(len(df_iot[df_iot['label'] == 'NORMAL'])),
            'avg_heart_rate': float(df_iot['heart_rate'].mean().round(2)),
            'avg_spo2': float(df_iot['oxygen_saturation'].mean().round(2)),
            'avg_temperature': float(df_iot['body_temperature'].mean().round(2)),
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(stats)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/patient-stream')
def patient_stream():
    """Server-Sent Events endpoint for real-time patient data streaming"""
    def generate():
        while True:
            try:
                df = fetch_iot_data(limit=10)
                if not df.empty:
                    latest_patients = df.tail(5).to_dict(orient='records')
                    yield f"data: {json.dumps(latest_patients)}\n\n"
                time.sleep(2)
            except Exception as e:
                print(f"Stream error: {e}")
                break
    
    return Response(generate(), mimetype='text/event-stream')

@app.route('/api/full-data')
def api_full_data():
    """API endpoint for full IoT data"""
    try:
        df_iot = fetch_iot_data(limit=5000)
        
        if df_iot.empty:
            return jsonify({'error': 'No data available'}), 500
        
        return jsonify(df_iot.to_dict(orient='records'))
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    session, cluster = get_cassandra_session()
    
    if session:
        try:
            # Test query
            result = session.execute(f"SELECT COUNT(*) FROM {IOT_DATA_TABLE} LIMIT 1")
            count = list(result)[0][0] if result else 0
            
            if cluster:
                cluster.shutdown()
            
            return jsonify({
                'status': 'healthy',
                'database': 'connected',
                'table': IOT_DATA_TABLE,
                'record_count': count
            }), 200
        
        except Exception as e:
            if cluster:
                cluster.shutdown()
            return jsonify({
                'status': 'unhealthy',
                'database': 'connected',
                'error': str(e)
            }), 503
    else:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected'
        }), 503

# -------------------------
# Run Flask App
# -------------------------
if __name__ == '__main__':
    print("🚀 Starting Real-Time Health Analytics Dashboard...")
    print("📊 Dashboard: http://localhost:5000")
    print("🔄 Real-time API: http://localhost:5000/api/realtime-data")
    print("🩺 Health Check: http://localhost:5000/health")
    print("\n✨ Press Ctrl+C to stop the server\n")
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)