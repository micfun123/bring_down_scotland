from flask import Flask, render_template, jsonify, request
import json
import os
from datetime import datetime
from sse_analyzer import SSEScotlandCapacityAnalyzer
import math

app = Flask(__name__)
app.config['SECRET_KEY'] = 'AHHHHHHHHHHHHHHHHHHHHHHHHHHHh'

# Global variable to store cached data
capacity_data = None
last_updated = None

def get_capacity_data(force_refresh=False):
    """
    Get capacity data, using cache unless force_refresh is True
    """
    global capacity_data, last_updated
    
    if capacity_data is None or force_refresh:
        print("Fetching fresh data from API...")
        analyzer = SSEScotlandCapacityAnalyzer()
        
        # Fetch data
        df = analyzer.fetch_scotland_data(limit=5000)
        
        if not df.empty:
            # Filter for Scotland
            scotland_df = analyzer.filter_scotland_data(df)
            
            if scotland_df.empty:
                scotland_df = df
            
            # Calculate totals
            totals = analyzer.calculate_capacity_totals(scotland_df)
            
            capacity_data = {
                'totals': totals,
                'records_count': len(scotland_df),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'summary': {
                    'accepted_capacity': totals.get('accepted_registered_capacity', {}).get('total_mw', 0),
                    'connected_capacity': totals.get('connected_registered_capacity', {}).get('total_mw', 0),
                    'max_export_capacity': totals.get('maximum_export_capacity', {}).get('total_mw', 0),
                    'max_import_capacity': totals.get('maximum_import_capacity', {}).get('total_mw', 0),
                    'grand_total': sum(totals[cap]['total_mw'] for cap in totals)
                }
            }
            
            last_updated = datetime.now()
            
            # Save to file for persistence
            with open('data_cache.json', 'w') as f:
                json.dump(capacity_data, f, indent=2)
                
        else:
            # Try to load from cache if API fails
            try:
                with open('data_cache.json', 'r') as f:
                    capacity_data = json.load(f)
                print("Loaded data from cache file")
            except FileNotFoundError:
                capacity_data = {
                    'totals': {},
                    'records_count': 0,
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'summary': {
                        'accepted_capacity': 0,
                        'connected_capacity': 0,
                        'max_export_capacity': 0,
                        'max_import_capacity': 0,
                        'grand_total': 0
                    }
                }
    
    return capacity_data

@app.route('/')
def index():
    """
    Main dashboard page
    """
    data = get_capacity_data()
    
    # Format numbers for display
    formatted_data = {
        'accepted_capacity': f"{data['summary']['accepted_capacity']:,.2f}",
        'connected_capacity': f"{data['summary']['connected_capacity']:,.2f}",
        'max_export_capacity': f"{data['summary']['max_export_capacity']:,.2f}",
        'max_import_capacity': f"{data['summary']['max_import_capacity']:,.2f}",
        'grand_total': f"{data['summary']['grand_total']:,.2f}",
        'records_count': f"{data['records_count']:,}",
        'last_updated': data['last_updated']
    }
    
    return render_template('index.html', data=formatted_data)

@app.route('/api/data')
def api_data():
    """
    JSON API endpoint for capacity data
    """
    data = get_capacity_data()
    return jsonify(data)

@app.route('/refresh')
def refresh_data():
    """
    Force refresh the data from API
    """
    data = get_capacity_data(force_refresh=True)
    
    formatted_data = {
        'accepted_capacity': f"{data['summary']['accepted_capacity']:,.2f}",
        'connected_capacity': f"{data['summary']['connected_capacity']:,.2f}",
        'max_export_capacity': f"{data['summary']['max_export_capacity']:,.2f}",
        'max_import_capacity': f"{data['summary']['max_import_capacity']:,.2f}",
        'grand_total': f"{data['summary']['grand_total']:,.2f}",
        'records_count': f"{data['records_count']:,}",
        'last_updated': data['last_updated']
    }
    
    return render_template('index.html', data=formatted_data, refreshed=True)

@app.route('/details')
def details():
    """
    Detailed data view
    """
    data = get_capacity_data()
    return render_template('data.html', data=data)

@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    """
    API endpoint to refresh data
    """
    data = get_capacity_data(force_refresh=True)
    return jsonify({
        'status': 'success',
        'message': 'Data refreshed successfully',
        'data': data
    })



@app.route('/api/game/calculate', methods=['POST'])
def calculate_blackout():
    """
    API endpoint to calculate blackout scenario
    """
    data = request.json
    kettle_count = data.get('kettle_count', 0)
    
    game_data = get_capacity_data()
    available_capacity = game_data['summary']['connected_capacity']
    
    # Game calculations
    kettle_power = 3.0  # kW per kettle
    total_kettle_power = (kettle_count * kettle_power) / 1000  # Convert to MW
    remaining_capacity = available_capacity - total_kettle_power
    blackout_percentage = min(100, max(0, (total_kettle_power / available_capacity) * 100))
    
    # Determine status
    if remaining_capacity <= 0:
        status = "blackout"
        message = "🏴󠁧󠁢󠁳󠁣󠁴󠁿 BLACKOUT! Scotland's grid has collapsed!"
    elif blackout_percentage > 90:
        status = "critical"
        message = "🔴 CRITICAL! Grid stability at risk!"
    elif blackout_percentage > 70:
        status = "warning"
        message = "🟡 WARNING! Grid under heavy strain!"
    else:
        status = "normal"
        message = "🟢 Grid operating normally"
    
    return jsonify({
        'kettle_count': kettle_count,
        'available_capacity': available_capacity,
        'kettle_power_mw': total_kettle_power,
        'remaining_capacity': max(0, remaining_capacity),
        'blackout_percentage': blackout_percentage,
        'status': status,
        'message': message,
        'kettles_to_blackout': math.ceil((available_capacity * 1000) / kettle_power)
    })

@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500

if __name__ == '__main__':
    # Pre-load data on startup
    print("Pre-loading capacity data...")
    get_capacity_data()
    
    app.run(debug=True, host='0.0.0.0', port=5000)