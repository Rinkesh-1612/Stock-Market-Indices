import pandas as pd
import plotly.graph_objects as go
import dash
from dash import dcc, html, callback, Input, Output, State
import dash_bootstrap_components as dbc
import os

dash.register_page(__name__, name="Top Movers", title="Top Gainers & Losers")

# --- Data Loading ---
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'final')
df, index_options = pd.DataFrame(), []
try:
    df_constituents = pd.read_csv(os.path.join(DATA_DIR, "master_constituents_list.csv"))
    df_prices = pd.read_csv(os.path.join(DATA_DIR, "final_all_historical_prices.csv"), index_col='Date', parse_dates=True)
    df_summary = pd.read_csv(os.path.join(DATA_DIR, "constituent_verification_summary.csv"))

    if len(df_prices) >= 366:
        perf_1d = (df_prices.iloc[-1] / df_prices.iloc[-2]) - 1
        perf_1w = (df_prices.iloc[-1] / df_prices.iloc[-6]) - 1
        perf_1m = (df_prices.iloc[-1] / df_prices.iloc[-22]) - 1
        perf_1y = (df_prices.iloc[-1] / df_prices.iloc[-253]) - 1
        df_perf = pd.DataFrame({'Perf_1D': perf_1d, 'Perf_1W': perf_1w, 'Perf_1M': perf_1m, 'Perf_1Y': perf_1y, 'Last Close': df_prices.iloc[-1]}).reset_index().rename(columns={'index':'Company Ticker'})
        df = pd.merge(df_constituents, df_perf, on="Company Ticker", how="inner").dropna()
        df_summary['Success Rate'] = df_summary['Success Rate'].astype(str).str.replace('%', '').astype(float)
        high_quality_indices = df_summary[df_summary['Success Rate'] >= 50.0]['Index Name'].unique()
        df = df[df['Index Name'].isin(high_quality_indices)]
        index_options = sorted(df['Index Name'].unique())
except FileNotFoundError as e:
    print(f"Top Movers page error: {e}")

# --- Page Layout ---
layout = dbc.Container([
    html.H1("Index Top Gainers & Losers", className="text-center my-4"),
    dbc.Card(dbc.CardBody([
        dbc.Row([
            dbc.Col(dcc.Dropdown(
                id='index-dropdown-movers',
                options=[{'label': i, 'value': i} for i in index_options],
                value='S&P 500 (USA)' if 'S&P 500 (USA)' in index_options else (index_options[0] if index_options else None),
            ), width=12, lg=6, className="mb-3 mb-lg-0"),
            dbc.Col(dbc.Tabs(id="period-tabs-movers", active_tab="Perf_1D", children=[
                dbc.Tab(label="Daily", tab_id="Perf_1D"), dbc.Tab(label="Weekly", tab_id="Perf_1W"),
                dbc.Tab(label="Monthly", tab_id="Perf_1M"), dbc.Tab(label="Yearly", tab_id="Perf_1Y"),
            ]), width=12, lg=6)
        ], align="center")
    ]), className="p-3 mb-4"),
    dbc.Row(dbc.Col(html.Div([
        dbc.Button("Show Analysis & Interpretation", id="collapse-movers-button", className="mb-3"),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(id="movers-interpretation-text")),
            id="collapse-movers", is_open=False,
        ),
    ]))),
    dbc.Row(dbc.Col(dbc.Spinner(dcc.Graph(id='gainer-loser-chart'))))
], fluid=True)

# --- Callbacks ---
@callback(
    Output('gainer-loser-chart', 'figure'),
    [Input('index-dropdown-movers', 'value'), Input('period-tabs-movers', 'active_tab')]
)
def update_chart(selected_index, selected_period):
    if not selected_index or df.empty: return {}
    filtered_df = df[df['Index Name'] == selected_index]
    if filtered_df.empty: return {}

    filtered_df = filtered_df.sort_values(by=selected_period, ascending=False)
    n = min(10, len(filtered_df) // 2)
    if n == 0 and len(filtered_df) > 0: n = 1
    
    top_n = filtered_df.head(n)
    bottom_n = filtered_df.tail(n)
    plot_df = pd.concat([top_n, bottom_n]).drop_duplicates().sort_values(by=selected_period)
    
    if plot_df.empty: return {}

    colors = ['#2ca02c' if x > 0 else '#d62728' for x in plot_df[selected_period]]
    fig = go.Figure(go.Bar(
        x=plot_df[selected_period] * 100, y=plot_df['Company Name'], orientation='h',
        marker_color=colors, text=[f"{p:.2%}" for p in plot_df[selected_period]], textposition='outside',
        customdata=plot_df[['Company Ticker', 'Last Close']],
        hovertemplate='<b>%{y}</b> (%{customdata[0]})<br>Performance: %{x:.2f}%<br>Last Close: %{customdata[1]:$,.2f}<extra></extra>'
    ))
    
    period_label = {"Perf_1D": "Daily", "Perf_1W": "Weekly", "Perf_1M": "Monthly", "Perf_1Y": "Yearly"}.get(selected_period)
    fig.update_layout(
        title=f"Top {n} Gainers & Losers for {selected_index} ({period_label} Performance)",
        xaxis_title="Performance (%)", yaxis_title="Company", template='plotly_dark',
        yaxis=dict(autorange="reversed"), height=800, margin=dict(l=250),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"
    )
    return fig

@callback(
    Output("collapse-movers", "is_open"),
    Input("collapse-movers-button", "n_clicks"),
    State("collapse-movers", "is_open"),
)
def toggle_collapse(n, is_open):
    if n: return not is_open
    return is_open

@callback(
    Output("movers-interpretation-text", "children"),
    [Input('index-dropdown-movers', 'value'), Input('period-tabs-movers', 'active_tab')]
)
def update_interpretation(selected_index, selected_period):
    if not selected_index or df.empty:
        return "Select an index and period to view analysis."
    
    filtered_df = df[df['Index Name'] == selected_index].sort_values(by=selected_period, ascending=False)
    if filtered_df.empty:
        return f"No performance data available for {selected_index}."

    period_label = {"Perf_1D": "Daily", "Perf_1W": "Weekly", "Perf_1M": "Monthly", "Perf_1Y": "Yearly"}.get(selected_period)
    top_gainer = filtered_df.iloc[0]
    top_loser = filtered_df.iloc[-1]

    return dcc.Markdown(f"""
    #### {period_label} Performance Analysis for {selected_index}
    This chart highlights the most significant constituent movers, providing a snapshot of the stocks driving or dragging down the index's performance over the selected period.

    **Key Insights for {period_label} Performance:**
    *   **Top Gainer:** The best-performing stock was **{top_gainer['Company Name']}** with a gain of **{top_gainer[selected_period]:.2%}**.
    *   **Top Loser:** The worst-performing stock was **{top_loser['Company Name']}** with a loss of **{top_loser[selected_period]:.2%}**.

    Analyzing top movers helps identify sources of market volatility and sector-specific trends. A large divergence between the top gainer and loser can indicate high dispersion in the market.
    """)