import os
import json
import time
from datetime import datetime, timedelta
import threading
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import plotly.express as px

# Import config
from config import (
    KAFKA_BOOTSTRAP_SERVERS, 
    KAFKA_TOPIC, 
    DASHBOARD_HOST, 
    DASHBOARD_PORT, 
    DASHBOARD_DEBUG,
    INSTRUMENTS,
    CURRENCIES,
    setup_logging
)

# Set up logging
logger = setup_logging("dashboard")

# Global storage
rate_history = {instrument: [] for instrument in INSTRUMENTS}
matrix_history = []
latest_matrix = None
system_status = {
    "last_update": None,
    "connection_status": "Initializing",
    "status_color": "orange",
    "update_count": 0,
    "start_time": time.time()
}

# Initialize Dash app
app = dash.Dash(__name__, title="FX Exchange Rate Dashboard")

# Dashboard layout
app.layout = html.Div([
    html.H1("FX Exchange Rate Dashboard", style={'textAlign': 'center'}),
    
    # System status panel
    html.Div([
        html.H3("System Status", style={'textAlign': 'center'}),
        html.Div(id='connection-status'),
        html.Div(id='last-update-time'),
        html.Div(id='uptime'),
        dcc.Interval(
            id='status-update-interval',
            interval=1000,  # Update every second
            n_intervals=0
        )
    ], style={'marginBottom': '20px', 'padding': '10px', 'border': '1px solid #ddd', 'borderRadius': '5px'}),
    
    # Main data visualization area
    html.Div([
        # Left column - Matrix heatmap
        html.Div([
            html.H3("Exchange Rate Matrix", style={'textAlign': 'center'}),
            dcc.Graph(id='matrix-heatmap'),
            dcc.Interval(
                id='matrix-update-interval',
                interval=5000,  # Update every 5 seconds
                n_intervals=0
            )
        ], style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'top'}),
        
        # Right column - Currency pair chart
        html.Div([
            html.H3("Currency Pair Trends", style={'textAlign': 'center'}),
            dcc.Dropdown(
                id='currency-pair-dropdown',
                options=[{'label': pair, 'value': pair} for pair in INSTRUMENTS],
                value=INSTRUMENTS[0] if INSTRUMENTS else None,
                clearable=False
            ),
            dcc.Graph(id='currency-pair-chart'),
            dcc.Interval(
                id='chart-update-interval',
                interval=5000,  # Update every 5 seconds
                n_intervals=0
            )
        ], style={'width': '49%', 'display': 'inline-block', 'vertical-align': 'top'})
    ]),
    
    # Arbitrage opportunities panel
    html.Div([
        html.H3("Arbitrage Opportunities", style={'textAlign': 'center'}),
        html.Div(id='arbitrage-table'),
        dcc.Interval(
            id='arbitrage-update-interval',
            interval=10000,  # Update every 10 seconds
            n_intervals=0
        )
    ], style={'marginTop': '20px', 'padding': '10px', 'border': '1px solid #ddd', 'borderRadius': '5px'})
])

# Start Kafka consumer in a background thread
def kafka_consumer_thread():
    """Background thread to consume Kafka messages and update data stores"""
    try:
        # Initialize consumer
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='fx-dashboard',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Update system status
        system_status["connection_status"] = "Connected to Kafka"
        system_status["status_color"] = "green"
        
        logger.info("Kafka consumer started successfully")
        
        # Process messages
        for message in consumer:
            try:
                # Extract message value
                data = message.value
                
                # Update system status
                system_status["last_update"] = datetime.now()
                system_status["update_count"] += 1
                
                # Process based on message type
                if data.get("type") == "PRICE":
                    process_price_tick(data)
                elif data.get("type") == "MATRIX":
                    process_matrix_update(data)
                elif data.get("type") == "CONSOLIDATED_RATES":
                    process_consolidated_rates(data)
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
        system_status["connection_status"] = f"Kafka error: {str(e)}"
        system_status["status_color"] = "red"

def process_price_tick(data):
    """Process a price tick message"""
    instrument = data.get("instrument")
    if not instrument or instrument not in INSTRUMENTS:
        return
    
    # Extract prices
    try:
        bid = float(data['bids'][0]['price'])
        ask = float(data['asks'][0]['price'])
        mid = data.get('mid_price', (bid + ask) / 2)
        timestamp = data.get('local_timestamp', time.time())
        
        # Add to history
        rate_history[instrument].append({
            'timestamp': timestamp,
            'datetime': datetime.fromtimestamp(timestamp),
            'mid': mid,
            'bid': bid,
            'ask': ask
        })
        
        # Keep only the last 1000 ticks per instrument
        if len(rate_history[instrument]) > 1000:
            rate_history[instrument] = rate_history[instrument][-1000:]
            
    except (KeyError, IndexError, ValueError, TypeError) as e:
        logger.warning(f"Error processing price tick for {instrument}: {e}")

def process_matrix_update(data):
    """Process a matrix update message"""
    global latest_matrix
    
    try:
        # Convert dict to DataFrame
        matrix_data = data.get('matrix', {})
        if not matrix_data:
            return
            
        matrix_df = pd.DataFrame(matrix_data)
        latest_matrix = matrix_df
        
        # Add to history with timestamp
        matrix_history.append({
            'timestamp': data.get('timestamp', time.time()),
            'datetime': datetime.fromtimestamp(data.get('timestamp', time.time())),
            'matrix': matrix_df
        })
        
        # Keep only the last 100 matrices
        if len(matrix_history) > 100:
            matrix_history = matrix_history[-100:]
            
    except Exception as e:
        logger.error(f"Error processing matrix update: {e}")

def process_consolidated_rates(data):
    """Process consolidated rates message"""
    rates = data.get('rates', {})
    timestamp = data.get('timestamp', time.time())
    
    for instrument, rate in rates.items():
        if instrument in INSTRUMENTS:
            # We don't have bid/ask in consolidated messages, so use mid for both
            rate_history[instrument].append({
                'timestamp': timestamp,
                'datetime': datetime.fromtimestamp(timestamp),
                'mid': rate,
                'bid': rate,  # Placeholder
                'ask': rate   # Placeholder
            })
            
            # Keep only the last 1000 ticks per instrument
            if len(rate_history[instrument]) > 1000:
                rate_history[instrument] = rate_history[instrument][-1000:]

# Dash callbacks
@app.callback(
    [Output('connection-status', 'children'),
     Output('connection-status', 'style'),
     Output('last-update-time', 'children'),
     Output('uptime', 'children')],
    Input('status-update-interval', 'n_intervals')
)
def update_status(n):
    """Update the system status display"""
    # Connection status
    status_style = {'padding': '10px', 'borderRadius': '5px', 'backgroundColor': system_status["status_color"], 'color': 'white'}
    status = html.Div(f"Status: {system_status['connection_status']}", style=status_style)
    
    # Last update time
    if system_status["last_update"]:
        last_update = html.Div(f"Last Update: {system_status['last_update'].strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        last_update = html.Div("Last Update: Never")
    
    # Uptime
    uptime_seconds = time.time() - system_status["start_time"]
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime = html.Div(f"Uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s | Total Updates: {system_status['update_count']}")
    
    return status, status_style, last_update, uptime

@app.callback(
    Output('matrix-heatmap', 'figure'),
    Input('matrix-update-interval', 'n_intervals')
)
def update_matrix_heatmap(n):
    """Update the exchange rate matrix heatmap"""
    if latest_matrix is None:
        # Return empty figure if no data
        return {
            'data': [],
            'layout': {
                'title': 'Waiting for matrix data...',
                'xaxis': {'title': 'Quote Currency'},
                'yaxis': {'title': 'Base Currency'}
            }
        }
    
    # Create heatmap
    fig = px.imshow(
        latest_matrix, 
        labels=dict(x="Quote Currency", y="Base Currency", color="Log Rate"),
        x=latest_matrix.columns,
        y=latest_matrix.index,
        color_continuous_scale='Viridis'
    )
    
    # Add text annotations
    annotations = []
    for i in range(len(latest_matrix.index)):
        for j in range(len(latest_matrix.columns)):
            value = latest_matrix.iloc[i, j]
            if pd.notna(value):
                # Convert log value to actual exchange rate for display
                rate = np.exp(value)
                text = f"{rate:.4f}"
                annotations.append({
                    'x': j,
                    'y': i,
                    'text': text,
                    'showarrow': False,
                    'font': {'color': 'white' if abs(value) > 0.1 else 'black'}
                })
    
    fig.update_layout(
        title="Exchange Rate Matrix (log values)",
        annotations=annotations
    )
    
    return fig

@app.callback(
    Output('currency-pair-chart', 'figure'),
    [Input('chart-update-interval', 'n_intervals'),
     Input('currency-pair-dropdown', 'value')]
)
def update_currency_pair_chart(n, currency_pair):
    """Update the currency pair time series chart"""
    if not currency_pair or not rate_history.get(currency_pair):
        # Return empty figure if no data
        return {
            'data': [],
            'layout': {
                'title': f'Waiting for {currency_pair} data...' if currency_pair else 'Select a currency pair',
                'xaxis': {'title': 'Time'},
                'yaxis': {'title': 'Rate'}
            }
        }
    
    # Get data for the selected pair
    pair_data = rate_history[currency_pair]
    
    if not pair_data:
        return {
            'data': [],
            'layout': {'title': f'No data available for {currency_pair}'}
        }
    
    # Create figure
    fig = go.Figure()
    
    # Extract data
    timestamps = [entry.get('datetime', datetime.fromtimestamp(entry['timestamp'])) 
                 for entry in pair_data]
    mids = [entry['mid'] for entry in pair_data]
    bids = [entry['bid'] for entry in pair_data]
    asks = [entry['ask'] for entry in pair_data]
    
    # Add traces
    fig.add_trace(go.Scatter(
        x=timestamps,
        y=mids,
        mode='lines',
        name='Mid Price',
        line={'color': 'rgb(31, 119, 180)', 'width': 2}
    ))
    
    fig.add_trace(go.Scatter(
        x=timestamps,
        y=bids,
        mode='lines',
        name='Bid Price',
        line={'color': 'rgb(44, 160, 44)', 'width': 1, 'dash': 'dot'}
    ))
    
    fig.add_trace(go.Scatter(
        x=timestamps,
        y=asks,
        mode='lines',
        name='Ask Price',
        line={'color': 'rgb(214, 39, 40)', 'width': 1, 'dash': 'dot'}
    ))
    
    # Update layout
    fig.update_layout(
        title=f"{currency_pair} Exchange Rate",
        xaxis_title="Time",
        yaxis_title="Rate",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

@app.callback(
    Output('arbitrage-table', 'children'),
    Input('arbitrage-update-interval', 'n_intervals')
)
def update_arbitrage_table(n):
    """Update the arbitrage opportunities table"""
    if latest_matrix is None:
        return html.Div("No matrix data available for arbitrage analysis")
    
    # Import the check_arbitrage function
    from matrix_builder import check_arbitrage
    
    # Find arbitrage opportunities
    opportunities = check_arbitrage(latest_matrix)
    
    if not opportunities:
        return html.Div("No arbitrage opportunities detected", 
                       style={'padding': '20px', 'textAlign': 'center'})
    
    # Create table
    headers = [
        html.Th("Path"), 
        html.Th("Log Deviation"), 
        html.Th("Potential Profit %")
    ]
    
    rows = []
    for opp in opportunities:
        rows.append(html.Tr([
            html.Td(opp['path']),
            html.Td(f"{opp['deviation']:.8f}"),
            html.Td(f"{opp['profit_percent']:.4f}%")
        ]))
    
    table = html.Table(
        [html.Thead(html.Tr(headers))] + [html.Tbody(rows)],
        style={'width': '100%', 'border': '1px solid #ddd', 'borderCollapse': 'collapse'}
    )
    
    return table

# Start the Kafka consumer thread
if __name__ == "__main__":
    # Start consumer thread
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    # Start Dash server
    app.run_server(host=DASHBOARD_HOST, port=DASHBOARD_PORT, debug=DASHBOARD_DEBUG)