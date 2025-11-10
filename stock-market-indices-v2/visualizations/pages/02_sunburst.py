import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html, callback, Input, Output, State
import os
import dash_bootstrap_components as dbc

dash.register_page(__name__, name="Index Distribution", title="Sunburst Distribution")

# --- Data Loading ---
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'final')
df, all_continents = pd.DataFrame(), []
try:
    df = pd.read_csv(os.path.join(DATA_DIR, "data_cleaned", "master_indices_list.csv"))
    df.dropna(subset=['Continent', 'Country'], inplace=True)
    df['count'] = 1
    all_continents = sorted(df['Continent'].unique())
except FileNotFoundError as e:
    print(f"Sunburst page error: {e}")

layout = dbc.Container([
    dbc.Row(dbc.Col(html.H1("Hierarchical Distribution of Global Stock Indices", className="text-center text-white mb-4"))),
    
    # Main row for controls and chart
    dbc.Row([
        # --- Column for Controls (Left Side) ---
        dbc.Col([
            dbc.Card(dbc.CardBody(
                dcc.Dropdown(
                    id='continent-filter-sunburst',
                    options=[{'label': i, 'value': i} for i in all_continents],
                    value=all_continents, 
                    multi=True, 
                    placeholder="Select continents..."
                )
            ), className="p-3 mb-4"),
            
            html.Div([
                dbc.Button("Show Analysis & Interpretation", id="collapse-sunburst-button", className="mb-3 w-100"),
                dbc.Collapse(
                    dbc.Card(dbc.CardBody(id="sunburst-interpretation-text")),
                    id="collapse-sunburst", is_open=False,
                ),
            ])
        ], width=12, md=4, lg=3), # Takes full width on small screens, 1/3 on large

        # --- Column for the Chart (Right Side) ---
        dbc.Col(
            dbc.Spinner(dcc.Graph(id='sunburst-chart', style={'height': '80vh'})),
            width=12, md=8, lg=9 # Takes full width on small screens, 2/3 on large
        )
    ])
], fluid=True)

# --- Callbacks ---
@callback(
    Output('sunburst-chart', 'figure'),
    Input('continent-filter-sunburst', 'value')
)
def update_sunburst(selected_continents):
    if not selected_continents or df.empty:
        return {}
    filtered_df = df[df['Continent'].isin(selected_continents)]
    fig = px.sunburst(
        filtered_df, path=['Continent', 'Country', 'Index Name'], values='count', color='Continent',
        template="plotly_dark"
    )
    fig.update_traces(hovertemplate='<b>%{label}</b><br>Number of Indices: %{value}<extra></extra>')
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig

@callback(
    Output("collapse-sunburst", "is_open"),
    Input("collapse-sunburst-button", "n_clicks"),
    State("collapse-sunburst", "is_open"),
)
def toggle_collapse(n, is_open):
    if n: return not is_open
    return is_open

@callback(
    Output("sunburst-interpretation-text", "children"),
    Input('continent-filter-sunburst', 'value')
)
def update_interpretation(selected_continents):
    if not selected_continents or df.empty:
        return "Select one or more continents to view analysis."

    filtered_df = df[df['Continent'].isin(selected_continents)]
    num_indices = len(filtered_df)
    num_countries = filtered_df['Country'].nunique()
    
    continent_counts = filtered_df['Continent'].value_counts()
    top_continent = continent_counts.index[0]
    
    country_counts = filtered_df['Country'].value_counts()
    top_country = country_counts.index[0]

    return dcc.Markdown(f"""
    #### Hierarchical Index Distribution Analysis
    This sunburst chart displays the distribution of **{num_indices}** major stock indices across **{num_countries}** countries in the selected continents. Each layer of the chart represents a different level of the hierarchy: Continent, Country, and Index Name.

    **Key Insights for the Current Selection:**
    *   **Most Represented Continent:** **{top_continent}** contains the highest number of indices in this selection.
    *   **Most Represented Country:** Globally, **{top_country}** has the most individual indices among the selected regions.
    
    This visualization is effective for understanding the structure and concentration of global financial markets. Clicking on a segment allows you to drill down and explore the hierarchy in more detail.
    """)