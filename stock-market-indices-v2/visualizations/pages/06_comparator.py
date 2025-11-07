import pandas as pd
import plotly.graph_objects as go
import dash
from dash import dcc, html, callback, Input, Output, State # Added State for the new callback
import dash_bootstrap_components as dbc
import os
from datetime import timedelta

dash.register_page(__name__, name="Index Comparator", title="Index Performance Comparator")

# --- Data Loading (unchanged, path was already correct) ---
def load_comparator_data():
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(SCRIPT_DIR, '..', '..', 'data', 'final')
    try:
        df_indices = pd.read_csv(os.path.join(DATA_DIR, "master_indices_list.csv"))
        df_indices = df_indices[df_indices['Ticker'] != '^MERV'].copy()
        df_prices = pd.read_csv(os.path.join(DATA_DIR, "final_all_historical_prices.csv"), index_col='Date', parse_dates=True)
        index_tickers = df_indices['Ticker'].tolist()
        df_prices = df_prices[df_prices.columns.intersection(index_tickers)].ffill()
    except FileNotFoundError:
        return pd.DataFrame(), pd.DataFrame(), {}, []

    ticker_map = pd.Series(df_indices['Index Name'].values, index=df_indices['Ticker']).to_dict()
    options = [{'label': row['Index Name'], 'value': row['Ticker']} for _, row in df_indices.iterrows()]
    return df_prices, df_indices, ticker_map, options

df_prices, df_indices, TICKER_TO_NAME_MAP, ALL_INDICES_OPTIONS = load_comparator_data()


# ==============================================================================
# === INTERPRETATION ADDED HERE ================================================
# ==============================================================================
# --- Define the Page Layout ---
layout = dbc.Container([
    # Page Title
    html.H1("Global Index Performance Comparator", className="text-center my-4"),
    
    # Control Card
    dbc.Card(dbc.CardBody([
        html.Label("Select Indices to Compare:", className="fw-bold"),
        dcc.Dropdown(
            id='index-dropdown-comparator', options=ALL_INDICES_OPTIONS, multi=True,
            value=['^GSPC', '^FTSE', '^N225', '^NSEI'], placeholder="Search and select indices...", className="my-3"
        ),
        html.Label("Select Date Range:", className="fw-bold"),
        dcc.DatePickerRange(
            id='date-picker-range-comparator',
            min_date_allowed=df_prices.index.min(), max_date_allowed=df_prices.index.max(),
            start_date=df_prices.index.max() - timedelta(days=365*3), end_date=df_prices.index.max(),
            className="d-block mt-2"
        ),
    ]), className="p-4"),
    
    # New Collapsible Interpretation Section
    dbc.Row(
        dbc.Col(
            html.Div([
                dbc.Button("Show Analysis & Interpretation", id="collapse-comparator-button", className="mb-3"),
                dbc.Collapse(
                    dbc.Card(dbc.CardBody(id="comparator-interpretation-text")),
                    id="collapse-comparator", is_open=False,
                ),
            ]), className="mt-4" # Margin top to space it from the card above
        )
    ),

    # The Performance Graph
    dbc.Spinner(dcc.Graph(id='performance-graph-comparator', style={'height': '70vh'}, className="mt-2"))
], fluid=True)


# --- Existing Callback for the Graph (unchanged) ---
@callback(
    Output('performance-graph-comparator', 'figure'),
    [Input('index-dropdown-comparator', 'value'), Input('date-picker-range-comparator', 'start_date'), Input('date-picker-range-comparator', 'end_date')]
)
def update_graph(selected_tickers, start_date, end_date):
    if not selected_tickers or df_prices.empty:
        return go.Figure().update_layout(title="Please select one or more indices.", template='plotly_dark')
    
    filtered_df = df_prices.loc[start_date:end_date]
    fig = go.Figure()
    for ticker in selected_tickers:
        if ticker in filtered_df.columns:
            series = filtered_df[ticker].dropna()
            if not series.empty:
                normalized = (series / series.iloc[0] - 1) * 100
                fig.add_trace(go.Scatter(x=normalized.index, y=normalized, mode='lines', name=TICKER_TO_NAME_MAP.get(ticker, ticker)))
    
    fig.update_layout(
        title="Normalized Index Performance (Rebased to 0%)", yaxis_title="Performance Change (%)",
        template='plotly_dark', paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        transition_duration=500
    )
    return fig


# --- New Callbacks for the Interpretation Section ---
@callback(
    Output("collapse-comparator", "is_open"),
    Input("collapse-comparator-button", "n_clicks"),
    State("collapse-comparator", "is_open"),
)
def toggle_collapse_comparator(n, is_open):
    if n:
        return not is_open
    return is_open

@callback(
    Output("comparator-interpretation-text", "children"),
    [Input('index-dropdown-comparator', 'value'), 
     Input('date-picker-range-comparator', 'start_date'), 
     Input('date-picker-range-comparator', 'end_date')]
)
def update_comparator_interpretation(selected_tickers, start_date, end_date):
    if not selected_tickers:
        return "Select indices to compare to see the analysis."

    names = [TICKER_TO_NAME_MAP.get(t, t) for t in selected_tickers]
    
    try:
        filtered_df = df_prices.loc[start_date:end_date, selected_tickers]
        if filtered_df.empty or len(filtered_df) < 2:
            return "Not enough data for the selected date range and tickers."
            
        performance = (filtered_df.iloc[-1] / filtered_df.iloc[0] - 1) * 100
        performance.index = performance.index.map(TICKER_TO_NAME_MAP)
        performance = performance.sort_values(ascending=False).dropna()

        if performance.empty:
            return "Could not calculate performance. Check if data is available for the full date range."

        best_performer = performance.index[0]
        best_perf_val = performance.iloc[0]
        worst_performer = performance.index[-1]
        worst_perf_val = performance.iloc[-1]

        return dcc.Markdown(f"""
            #### Comparative Performance Analysis
            
            This chart normalizes the performance of selected indices to a common starting point of 0%, allowing for a direct comparison of their relative growth over the chosen period from **{start_date[:10]}** to **{end_date[:10]}**.
            
            **Selected Indices:** {', '.join(names)}.
            
            **Key Insights:**
            
            *   **Top Performer:** Over this period, the **{best_performer}** was the strongest performer, registering a growth of **{best_perf_val:.2f}%**.
            *   **Lagging Performer:** The **{worst_performer}** showed the weakest relative performance, with a change of **{worst_perf_val:.2f}%**.
            
            This analysis is crucial for understanding regional market trends and identifying outperforming asset classes. For example, a divergence between a technology-heavy index and a broad-market index can signal shifts in investor sentiment.
        """)
    except Exception as e:
        return f"An error occurred during analysis: {str(e)}"
# ==============================================================================
# === END OF INTERPRETATION SECTION ============================================
# ==============================================================================