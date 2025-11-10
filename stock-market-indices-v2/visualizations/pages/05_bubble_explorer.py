import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html, callback, Input, Output, State
import os
import dash_bootstrap_components as dbc

dash.register_page(__name__, name="Constituent Explorer", title="Bubble Explorer")

# --- Data Loading ---
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'final')
df, index_options = pd.DataFrame(), []
try:
    df_constituents = pd.read_csv(os.path.join(DATA_DIR, "master_constituents_list.csv"))
    df_info = pd.read_csv(os.path.join(DATA_DIR, "final_company_info_usd.csv"))
    df_summary = pd.read_csv(os.path.join(DATA_DIR, "constituent_verification_summary.csv"))

    df = pd.merge(df_constituents, df_info, on="Company Ticker", how="inner")
    df.dropna(subset=['MarketCap_USD', 'Sector'], inplace=True)
    df_summary['Success Rate'] = df_summary['Success Rate'].astype(str).str.replace('%', '').astype(float)
    high_quality_indices = df_summary[df_summary['Success Rate'] >= 50.0]['Index Name'].unique()
    df = df[df['Index Name'].isin(high_quality_indices)]
    index_options = sorted(df['Index Name'].unique())
except FileNotFoundError as e:
    print(f"Bubble Explorer page error: {e}")

# --- Page Layout ---
layout = dbc.Container([
    html.H1("Stock Index Constituent Explorer", className="text-center my-4"),
    dbc.Card(dbc.CardBody(
        dcc.Dropdown(
            id='index-dropdown-bubble',
            options=[{'label': i, 'value': i} for i in index_options],
            value='S&P 500 (USA)' if 'S&P 500 (USA)' in index_options else (index_options[0] if index_options else None)
        )
    ), className="p-3 mb-4"),
    dbc.Row(dbc.Col(html.Div([
        dbc.Button("Show Analysis & Interpretation", id="collapse-bubble-button", className="mb-3"),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(id="bubble-interpretation-text")),
            id="collapse-bubble", is_open=False,
        ),
    ]))),
    dbc.Row(dbc.Col(dbc.Spinner(dcc.Graph(id='market-cap-bubble-chart', style={'height': '75vh'}))))
], fluid=True)

# --- Callbacks ---
@callback(
    Output('market-cap-bubble-chart', 'figure'), 
    Input('index-dropdown-bubble', 'value')
)
def update_bubble_chart(selected_index):
    if not selected_index or df.empty: 
        return {}
        
    filtered_df = df[df['Index Name'] == selected_index].copy()
    
    # ==============================================================================
    # === THE FIX IS HERE: Create a custom formatting function =====================
    # ==============================================================================
    def format_market_cap_usd(mc_usd):
        """Custom function to format market cap into a readable string with B for Billions."""
        if pd.isna(mc_usd):
            return "N/A"
        if mc_usd >= 1e12:
            return f"${mc_usd / 1e12:.2f}T"  # Trillions
        if mc_usd >= 1e9:
            return f"${mc_usd / 1e9:.2f}B"  # Billions
        if mc_usd >= 1e6:
            return f"${mc_usd / 1e6:.2f}M"  # Millions
        return f"${mc_usd / 1e3:.2f}K"      # Thousands

    # Apply this function to create a new column for the hover text
    filtered_df['MarketCapFormatted'] = filtered_df['MarketCap_USD'].apply(format_market_cap_usd)
    # ==============================================================================
    # === END OF FIX ===============================================================
    # ==============================================================================

    fig = px.scatter(
        filtered_df, 
        x="Company Name", 
        y="MarketCap_USD", 
        size="MarketCap_USD", 
        color="Sector",
        hover_name="Company Name", 
        # Pass the new formatted column to custom_data
        custom_data=['Company Ticker', 'MarketCapFormatted', 'Sector', 'Industry'],
        log_y=True, 
        size_max=80, 
        template="plotly_dark"
    )
    
    fig.update_layout(
        title=f"Market Cap Distribution for {selected_index} (USD)",
        xaxis={'title': '', 'showticklabels': False, 'showgrid': False},
        yaxis={'title': 'Market Cap (USD, Log Scale)'}, 
        legend_title_text='Sector',
        paper_bgcolor="rgba(0,0,0,0)", 
        plot_bgcolor="rgba(0,0,0,0)"
    )
    
    # Update the hovertemplate to use the new pre-formatted string
    fig.update_traces(
        hovertemplate=(
            '<b>%{hovertext}</b> (%{customdata[0]})<br>'
            'Market Cap (USD): %{customdata[1]}<br>' # This now uses our 'MarketCapFormatted' column
            'Sector: %{customdata[2]}<br>'
            'Industry: %{customdata[3]}'
            '<extra></extra>'
        )
    )
    return fig

@callback(
    Output("collapse-bubble", "is_open"),
    Input("collapse-bubble-button", "n_clicks"),
    State("collapse-bubble", "is_open"),
)
def toggle_collapse(n, is_open):
    if n: return not is_open
    return is_open

@callback(
    Output("bubble-interpretation-text", "children"),
    Input('index-dropdown-bubble', 'value')
)
def update_interpretation(selected_index):
    if not selected_index or df.empty:
        return "Select an index to view analysis."
    
    filtered_df = df[df['Index Name'] == selected_index]
    if filtered_df.empty: return f"No data for {selected_index}."

    largest_company = filtered_df.loc[filtered_df['MarketCap_USD'].idxmax()]
    sector_mcap = filtered_df.groupby('Sector')['MarketCap_USD'].sum().sort_values(ascending=False)
    dominant_sector = sector_mcap.index[0]
    dominant_sector_mcap = sector_mcap.iloc[0] / 1e9 # in Billions

    return dcc.Markdown(f"""
    #### Constituent Analysis for {selected_index}
    This bubble chart illustrates the market capitalization of each company within the selected index. The size of each bubble corresponds to its market cap (in USD), and the color represents its sector. The y-axis uses a logarithmic scale to accommodate the wide range of company sizes.

    **Key Insights for {selected_index}:**
    *   **Largest Constituent:** The most valuable company in this index is **{largest_company['Company Name']}**, with a market capitalization of approximately **${largest_company['MarketCap_USD']/1e9:.2f} Billion USD**.
    *   **Most Dominant Sector:** The **{dominant_sector}** sector holds the largest combined market capitalization, totaling approximately **${dominant_sector_mcap:.2f} Billion USD**.

    This visualization reveals the index's composition and concentration. An index heavily weighted towards a single sector or a few mega-cap stocks may behave differently than a more diversified one.
    """)