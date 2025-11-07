import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html, callback, Input, Output, State # Added State for the new callback
import os
import logging
import dash_bootstrap_components as dbc

# --- Register this page ---
dash.register_page(__name__, path='/', name="Global Map", title="Global Indices Map")

# --- Data Loading (already updated to /final) ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', '..', 'data', 'final')
MASTER_INDICES_FILE = os.path.join(DATA_DIR, "master_indices_list.csv")

try:
    df = pd.read_csv(MASTER_INDICES_FILE)
    df.dropna(subset=['Latitude', 'Longitude', 'Continent'], inplace=True)
except FileNotFoundError:
    logging.error(f"FATAL: Master index list not found at '{MASTER_INDICES_FILE}'.")
    df = pd.DataFrame() # Create empty df to avoid crash

aggregated_df = df.groupby(['Country', 'Continent', 'Latitude', 'Longitude']).agg(
    Index_Count=('Ticker', 'size'),
    Hover_Text=('Index Name', lambda names: '<br>'.join(names.head(15)) + (f'<br>... and {len(names) - 15} more.' if len(names) > 15 else ''))
).reset_index()

# --- Create the Plotly Figure (unchanged) ---
fig = px.scatter_mapbox(
    data_frame=aggregated_df, lat="Latitude", lon="Longitude", color="Continent",
    size="Index_Count", hover_name="Country", custom_data=["Hover_Text", "Index_Count"],
    size_max=40, zoom=1.2, center={"lat": 30, "lon": 20}
)
fig.update_traces(
    hovertemplate="<b>%{hovertext}</b><br><br>" +
                  "Number of Indices: %{customdata[1]}<br>" +
                  "--------------------<br>" +
                  "%{customdata[0]}" + "<extra></extra>"
)
fig.update_layout(
    title={'text': "Global Distribution of Major Stock Market Indices", 'y':0.95, 'x':0.5, 'xanchor': 'center', 'yanchor': 'top', 'font': {'color': 'white', 'size': 20}},
    mapbox_style="carto-darkmatter", margin={"r":0, "t":40, "l":0, "b":0},
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    legend=dict(title_text='Continent', orientation="v", yanchor="top", y=0.98, xanchor="right", x=0.98, bgcolor="rgba(0, 0, 0, 0.7)", bordercolor="rgba(255, 255, 255, 0.5)", borderwidth=1, font=dict(color="white"))
)


# ==============================================================================
# === INTERPRETATION ADDED HERE ================================================
# ==============================================================================
# --- Define the Page Layout ---
layout = dbc.Container([
    # Page Title
    dbc.Row(dbc.Col(html.H1("Global Financial Indices Map", className="text-center text-white mb-4"))),
    
    # New Collapsible Interpretation Section
    dbc.Row(
        dbc.Col(
            html.Div([
                dbc.Button("Show Analysis & Interpretation", id="collapse-map-button", className="mb-3"),
                dbc.Collapse(
                    dbc.Card(dbc.CardBody(dcc.Markdown("""
                        #### What This Map Shows
                        This map visualizes the geographical distribution of major global stock market indices. Each bubble represents a country, and its size corresponds to the number of major indices tracked in that location.
                        
                        **Methodology:**
                        Indices were scraped from Wikipedia and geolocated based on their primary country of operation. The bubble size is a direct count of indices per country from the curated `master_indices_list.csv`.
                        
                        **Key Insights:**
                        1.  **Market Concentration:** There is a significant concentration of major indices in North America (specifically the USA) and Western Europe. This reflects the historical dominance and maturity of these financial markets.
                        2.  **Asian Growth:** East Asia, particularly China, Japan, and India, also shows a strong presence, highlighting the region's economic importance.
                        3.  **Emerging Markets:** The representation in South America and Africa is comparatively sparse, indicating fewer globally recognized headline indices or more nascent market structures.
                        
                        **Conclusion:** The map reveals that while finance is global, the epicenters of market-defining indices remain concentrated in historically developed economic zones.
                    """))),
                    id="collapse-map", is_open=False,
                ),
            ])
        )
    ),
    
    # The Map Graph
    dbc.Row(dbc.Col(dcc.Graph(id='global-map', figure=fig, style={'height': '80vh'})))
], fluid=True)


# --- New Callback for the Collapse Button ---
@callback(
    Output("collapse-map", "is_open"),
    Input("collapse-map-button", "n_clicks"),
    State("collapse-map", "is_open"),
)
def toggle_collapse_map(n, is_open):
    if n:
        return not is_open
    return is_open
# ==============================================================================
# === END OF INTERPRETATION SECTION ============================================
# ==============================================================================