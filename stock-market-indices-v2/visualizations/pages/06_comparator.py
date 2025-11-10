import pandas as pd
import plotly.graph_objects as go
import dash
from dash import dcc, html, callback, Input, Output, State
import dash_bootstrap_components as dbc
import os
from datetime import datetime, timedelta
import zipfile
import io

dash.register_page(__name__, name="Index Comparator", title="Index Performance Comparator")

# --- Corrected Data Loading Functions ---
def load_price_data(data_dir):
    df_prices = pd.DataFrame()
    try:
        file_path = os.path.join(data_dir, "final_all_historical_prices_parquet.zip")
        with zipfile.ZipFile(file_path, 'r') as zf:
            parquet_filename = [f for f in zf.namelist() if f.endswith('.parquet')][0]
            with zf.open(parquet_filename) as pf:
                df_prices = pd.read_parquet(pf, engine='pyarrow')
        if 'Date' in df_prices.columns:
            df_prices = df_prices.set_index('Date')
        df_prices.index = pd.to_datetime(df_prices.index)
        for col in df_prices.columns:
            df_prices[col] = pd.to_numeric(df_prices[col], errors='coerce')
    except Exception as e:
        print(f"Error loading price data in Comparator page: {e}")
    return df_prices

def load_comparator_data():
    DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'final')
    df_prices = load_price_data(DATA_DIR)
    ticker_map, options, initial_start_date, initial_end_date = {}, [], None, None
    try:
        df_indices = pd.read_csv(os.path.join(DATA_DIR, 'data_cleaned', "master_indices_list.csv"))
        df_indices = df_indices[df_indices['Ticker'] != '^MERV'].copy()
        if not df_prices.empty:
            desired_tickers = set(df_indices['Ticker'])
            available_tickers = set(df_prices.columns)
            plottable_tickers = sorted(list(desired_tickers.intersection(available_tickers)))
            df_prices = df_prices[plottable_tickers].ffill()
            df_indices_plottable = df_indices[df_indices['Ticker'].isin(plottable_tickers)]
            ticker_map = pd.Series(df_indices_plottable['Index Name'].values, index=df_indices_plottable['Ticker']).to_dict()
            options = [{'label': row['Index Name'], 'value': row['Ticker']} for _, row in df_indices_plottable.iterrows()]
            initial_start_date = df_prices.index.max() - timedelta(days=365*3)
            initial_end_date = df_prices.index.max()
    except Exception as e:
        print(f"Error during data orchestration in Comparator page: {e}")
    return df_prices, ticker_map, options, initial_start_date, initial_end_date

df_prices, TICKER_TO_NAME_MAP, ALL_INDICES_OPTIONS, initial_start_date, initial_end_date = load_comparator_data()

# ==============================================================================
# === THE FIX IS HERE: New Layout and Notification Area ========================
# ==============================================================================
# --- Define the Page Layout ---
layout = dbc.Container([
    html.H1("Global Index Performance Comparator", className="text-center my-4"),
    dbc.Row([
        # --- Column for Controls ---
        dbc.Col([
            dbc.Card(dbc.CardBody([
                html.Label("Select Indices to Compare:", className="fw-bold"),
                dcc.Dropdown(
                    id='index-dropdown-comparator', options=ALL_INDICES_OPTIONS, multi=True,
                    value=['^GSPC', '^FTSE', '^N225', '^NSEI'], placeholder="Search and select indices...", className="my-3"
                ),
                html.Label("Select Date Range:", className="fw-bold"),
                dcc.DatePickerRange(
                    id='date-picker-range-comparator',
                    min_date_allowed=df_prices.index.min() if not df_prices.empty else datetime.now().date(),
                    max_date_allowed=initial_end_date if initial_end_date else datetime.now().date(),
                    start_date=initial_start_date if initial_start_date else datetime.now().date() - timedelta(days=365*3),
                    end_date=initial_end_date if initial_end_date else datetime.now().date(),
                    className="d-block mt-2"
                ),
            ]), className="p-4"),
            
            # --- New Interpretation and Notification Area ---
            html.Div([
                dbc.Button("Show Analysis & Interpretation", id="collapse-comparator-button", className="mb-3 w-100"),
                dbc.Collapse(
                    dbc.Card(dbc.CardBody(id="comparator-interpretation-text")),
                    id="collapse-comparator", is_open=False,
                ),
            ], className="mt-4"),
            
            # --- This is the new notification area ---
            html.Div(id="comparator-notification-area", className="mt-4")

        ], width=12, lg=4),

        # --- Column for the Chart ---
        dbc.Col(
            dbc.Spinner(dcc.Graph(id='performance-graph-comparator', style={'height': '80vh'})),
            width=12, lg=8
        )
    ])
], fluid=True)
# ==============================================================================
# === END OF FIX ===============================================================
# ==============================================================================


# --- Callback for the Graph (Now returns two outputs) ---
@callback(
    Output('performance-graph-comparator', 'figure'),
    Output('comparator-notification-area', 'children'), # New output for notifications
    [Input('index-dropdown-comparator', 'value'), 
     Input('date-picker-range-comparator', 'start_date'), 
     Input('date-picker-range-comparator', 'end_date')]
)
def update_graph(selected_tickers, start_date, end_date):
    if not selected_tickers or df_prices.empty:
        return go.Figure().update_layout(title="Please select one or more indices.", template='plotly_dark'), None

    filtered_df = df_prices.loc[start_date:end_date]
    fig = go.Figure()
    
    plotted_tickers = []
    skipped_tickers = []

    for ticker in selected_tickers:
        if ticker in filtered_df.columns:
            series = filtered_df[ticker].dropna()
            # This check is the reason tickers don't plot
            if not series.empty:
                normalized = (series / series.iloc[0] - 1) * 100
                fig.add_trace(go.Scatter(x=normalized.index, y=normalized, mode='lines', name=TICKER_TO_NAME_MAP.get(ticker, ticker)))
                plotted_tickers.append(ticker)
            else:
                skipped_tickers.append(ticker)
        else:
            skipped_tickers.append(ticker)
    
    # --- Create the notification message ---
    notification = None
    if skipped_tickers:
        skipped_names = [TICKER_TO_NAME_MAP.get(t, t) for t in skipped_tickers]
        notification = dbc.Alert(
            [
                html.H5("Note", className="alert-heading"),
                "The following indices could not be plotted as they have no data in the selected date range:",
                html.Ul([html.Li(name) for name in skipped_names])
            ],
            color="warning",
            dismissable=True,
        )

    fig.update_layout(
        title="Normalized Index Performance (Rebased to 0%)", yaxis_title="Performance Change (%)",
        template='plotly_dark', paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        transition_duration=300
    )
    return fig, notification


# --- Callbacks for Interpretation Section (unchanged) ---
@callback(
    Output("collapse-comparator", "is_open"),
    Input("collapse-comparator-button", "n_clicks"),
    State("collapse-comparator", "is_open"),
)
def toggle_collapse_comparator(n, is_open):
    if n: return not is_open
    return is_open

@callback(
    Output("comparator-interpretation-text", "children"),
    [Input('index-dropdown-comparator', 'value'), 
     Input('date-picker-range-comparator', 'start_date'), 
     Input('date-picker-range-comparator', 'end_date')]
)
def update_comparator_interpretation(selected_tickers, start_date, end_date):
    if not selected_tickers: return "Select indices to compare to see the analysis."
    names = [TICKER_TO_NAME_MAP.get(t, t) for t in selected_tickers]
    try:
        # We only analyze tickers that can actually be plotted in the selected range
        plottable_tickers = [t for t in selected_tickers if not df_prices.loc[start_date:end_date, t].dropna().empty]
        if not plottable_tickers: return "No data available for any selected index in this date range."
        
        filtered_df = df_prices.loc[start_date:end_date, plottable_tickers]
        performance = (filtered_df.iloc[-1] / filtered_df.iloc[0] - 1) * 100
        performance.index = performance.index.map(TICKER_TO_NAME_MAP)
        performance = performance.sort_values(ascending=False).dropna()
        best_performer = performance.index[0]
        best_perf_val = performance.iloc[0]
        worst_performer = performance.index[-1]
        worst_perf_val = performance.iloc[-1]
        return dcc.Markdown(f"""
            #### Comparative Performance Analysis
            This chart normalizes the performance of selected indices to a common starting point of 0%, allowing for a direct comparison of their relative growth over the chosen period from **{start_date[:10]}** to **{end_date[:10]}**.
            **Key Insights (for plotted indices):**
            *   **Top Performer:** The **{best_performer}** was the strongest, registering a growth of **{best_perf_val:.2f}%**.
            *   **Lagging Performer:** The **{worst_performer}** showed the weakest relative performance, with a change of **{worst_perf_val:.2f}%**.
            This analysis is crucial for understanding regional market trends and identifying outperforming asset classes.
        """)
    except Exception as e:
        return f"An error occurred during analysis: {str(e)}"