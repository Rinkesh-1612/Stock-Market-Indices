import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html, callback, Input, Output, State
import os
import dash_bootstrap_components as dbc
import zipfile # <-- ADD THIS IMPORT
import io      # <-- ADD THIS IMPORT


dash.register_page(__name__, name="Market Treemap", title="Market Treemap")

df, index_options = pd.DataFrame(), [] # Initialize defaults

try:
    # 1. Load metadata files
    DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'final')
    df_constituents = pd.read_csv(os.path.join(DATA_DIR, "master_constituents_list.csv"))
    df_info = pd.read_csv(os.path.join(DATA_DIR, "final_company_info_usd.csv"))
    df_summary = pd.read_csv(os.path.join(DATA_DIR, "constituent_verification_summary.csv"))

    # 2. Load and clean price data from zipped Parquet
    df_prices = pd.DataFrame() # Initialize as empty
    prices_path = os.path.join(DATA_DIR, "final_all_historical_prices_parquet.zip")
    with zipfile.ZipFile(prices_path, 'r') as zf:
        parquet_filename = [f for f in zf.namelist() if f.endswith('.parquet')][0]
        with zf.open(parquet_filename) as pf:
            df_prices = pd.read_parquet(pf, engine='pyarrow')

    # **CRITICAL FIX:** Clean the loaded DataFrame
    if 'Date' in df_prices.columns:
        df_prices = df_prices.set_index('Date')
    df_prices.index = pd.to_datetime(df_prices.index)
    for col in df_prices.columns:
        df_prices[col] = pd.to_numeric(df_prices[col], errors='coerce')

    # 3. Process data ONLY if price data is valid and sufficient
    if not df_prices.empty and len(df_prices) >= 22:
        df_merged = pd.merge(df_constituents, df_info, on="Company Ticker", how="inner")
        
        perf_1d = (df_prices.iloc[-1] / df_prices.iloc[-2]) - 1
        perf_1w = (df_prices.iloc[-1] / df_prices.iloc[-6]) - 1
        perf_1m = (df_prices.iloc[-1] / df_prices.iloc[-22]) - 1
        df_perf = pd.DataFrame({'Perf_1D': perf_1d, 'Perf_1W': perf_1w, 'Perf_1M': perf_1m}).reset_index().rename(columns={'index':'Company Ticker'})
        
        df = pd.merge(df_merged, df_perf, on="Company Ticker", how="inner")
        df.dropna(subset=['MarketCap_USD', 'Sector', 'Perf_1D'], inplace=True)
        
        df_summary['Success Rate'] = df_summary['Success Rate'].astype(str).str.replace('%', '').astype(float)
        high_quality_indices = df_summary[df_summary['Success Rate'] >= 50.0]['Index Name'].unique()
        df = df[df['Index Name'].isin(high_quality_indices)]
        
        index_options = sorted(df['Index Name'].unique())

except Exception as e:
    print(f"CRITICAL ERROR in treemap page: {e}")
# --- New Layout with Interpretation Section ---
layout = dbc.Container([
    dbc.Row(dbc.Col(html.H1("Market Performance Treemap", className="text-center text-white my-4"))),
    dbc.Card(
        dbc.CardBody([
            dbc.Row([
                dbc.Col(dcc.Dropdown(
                    id='index-dropdown-treemap',
                    options=[{'label': i, 'value': i} for i in index_options],
                    value='S&P 500 (USA)' if 'S&P 500 (USA)' in index_options else (index_options[0] if index_options else None),
                ), width=12, lg=6, className="mb-3 mb-lg-0"),
                dbc.Col(dcc.RadioItems(
                    id='period-selector-treemap',
                    options=[
                        {'label': '1-Day', 'value': 'Perf_1D'}, {'label': '1-Week', 'value': 'Perf_1W'},
                        {'label': '1-Month', 'value': 'Perf_1M'},
                    ],
                    value='Perf_1D', className="btn-group", inputClassName="btn-check",
                    labelClassName="btn btn-outline-primary", labelStyle={"margin": "0"},
                ), width=12, lg=6, className="d-flex justify-content-center align-items-center"),
            ], align="center"),
        ]),
        className="p-3 mb-4"
    ),

    # --- Interpretation Section ---
    dbc.Row(
        dbc.Col(
            html.Div([
                dbc.Button("Show Analysis & Interpretation", id="collapse-treemap-button", className="mb-2"),
                dbc.Collapse(
                    dbc.Card(dbc.CardBody(id="treemap-interpretation-text")),
                    id="collapse-treemap", is_open=False,
                ),
            ])
        )
    ),
    
    dbc.Row(dbc.Col(dbc.Spinner(dcc.Graph(id='market-treemap', style={'height': '75vh'}))))
], fluid=True)


# --- Callback for the Treemap Figure (same as before) ---
@callback(
    Output('market-treemap', 'figure'),
    [Input('index-dropdown-treemap', 'value'), Input('period-selector-treemap', 'value')]
)
def update_treemap(selected_index, selected_period):
    if not selected_index or df.empty:
        return {} # Return empty to avoid errors
    
    filtered_df = df[df['Index Name'] == selected_index]
    if filtered_df.empty: return {}

    fig = px.treemap(
        filtered_df, path=[px.Constant(selected_index), 'Sector', 'Company Name'],
        values='MarketCap_USD', color=selected_period, hover_name="Company Name",
        custom_data=['Company Ticker', selected_period],
        color_continuous_scale='RdYlGn', color_continuous_midpoint=0
    )
    fig.update_traces(hovertemplate='<b>%{label}</b> (%{customdata[0]})<br>Market Cap: %{value:$,.2s}<br>Performance: %{customdata[1]:.2%}<extra></extra>')
    fig.update_layout(
        title={'text': f"Constituent Performance for {selected_index}", 'font': {'color': 'white', 'size': 20}, 'x': 0.5},
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=50, l=25, r=25, b=25), transition_duration=300
    )
    return fig

# --- New Callback for the Collapsible Section ---
@callback(
    Output("collapse-treemap", "is_open"),
    Input("collapse-treemap-button", "n_clicks"),
    State("collapse-treemap", "is_open"),
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

# --- New Callback for DYNAMIC Interpretation Text ---
@callback(
    Output("treemap-interpretation-text", "children"),
    [Input('index-dropdown-treemap', 'value'), Input('period-selector-treemap', 'value')]
)
def update_interpretation(selected_index, selected_period):
    if not selected_index or df.empty:
        return "Select an index to see the analysis."

    filtered_df = df[df['Index Name'] == selected_index].copy()
    if filtered_df.empty:
        return f"No constituent data available for {selected_index}."

    period_map = {'Perf_1D': 'daily', 'Perf_1W': 'weekly', 'Perf_1M': 'monthly'}
    period_text = period_map.get(selected_period, 'selected')

    # Calculate key insights
    filtered_df['weighted_perf'] = filtered_df[selected_period] * filtered_df['MarketCap_USD']
    overall_perf = filtered_df['weighted_perf'].sum() / filtered_df['MarketCap_USD'].sum()
    
    sector_perf = filtered_df.groupby('Sector').apply(
        lambda x: (x[selected_period] * x['MarketCap_USD']).sum() / x['MarketCap_USD'].sum()
    ).sort_values(ascending=False)
    
    best_sector = sector_perf.index[0]
    worst_sector = sector_perf.index[-1]
    best_perf = sector_perf.iloc[0]
    worst_perf = sector_perf.iloc[-1]

    # Structure the interpretation using dcc.Markdown for formatting
    interpretation = dcc.Markdown(f"""
    #### Analysis of {selected_index}

    This treemap visualizes the market capitalization and **{period_text} performance** of the companies within the **{selected_index}**.
    
    *   **Size of Rectangle:** Represents the company's market capitalization in USD. Larger companies like *{filtered_df.sort_values('MarketCap_USD', ascending=False).iloc[0]['Company Name']}* occupy more space.
    *   **Color:** Indicates performance. Green signifies positive returns, while red indicates negative returns.
    
    **Key Insights:**
    
    1.  **Overall Market-Weighted Performance:** The index shows a market-cap-weighted **{period_text} performance of {overall_perf:.2%}**.
    2.  **Top Performing Sector:** The **{best_sector}** sector was the strongest performer, with a weighted average return of **{best_perf:.2%}**.
    3.  **Lagging Sector:** Conversely, the **{worst_sector}** sector was the weakest, posting a weighted average return of **{worst_perf:.2%}**.
    
    This view allows for a quick assessment of which sectors and major companies are driving the index's movement over the selected period.
    """)
    return interpretation