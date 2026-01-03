import pandas as pd
import plotly.graph_objects as go
        
        df_prices_filtered = df_prices[common_tickers]
        df_indices_filtered = df_indices[df_indices['Ticker'].isin(common_tickers)]
        df_prices_filled = df_prices_filtered.ffill().bfill()
        
        if len(df_prices_filled.columns) > 40:
            top_indices = df_prices_filled.count().nlargest(40).index
            df_prices_filled = df_prices_filled[top_indices]

        df_returns = df_prices_filled.pct_change().dropna()
        non_volatile_indices = df_returns.columns[df_returns.std() < 1e-8] 
        if len(non_volatile_indices) > 0:
            df_returns = df_returns.drop(columns=non_volatile_indices)
        
        correlation_matrix = df_returns.corr()
        
        if not correlation_matrix.empty and len(correlation_matrix) > 1:
            dist_matrix = pdist(correlation_matrix.values)
            linkage_matrix = linkage(dist_matrix, method='ward')
            ordered_indices = leaves_list(linkage_matrix)
            ordered_columns = correlation_matrix.columns[ordered_indices]
            correlation_matrix = correlation_matrix.reindex(index=ordered_columns, columns=ordered_columns)

        ticker_to_name = pd.Series(df_indices_filtered['Index Name'].values, index=df_indices_filtered['Ticker']).to_dict()
        correlation_matrix.rename(columns=ticker_to_name, index=ticker_to_name, inplace=True)

except Exception as e:
    print(f"CRITICAL ERROR loading data for heatmap: {e}")
# --- Page Layout ---
layout = dbc.Container([
    dbc.Row(dbc.Col(html.H1("Index Correlation Heatmap", className="text-center text-white my-4"))),
    
    dbc.Row(dbc.Col(html.Div([
        dbc.Button("Show Analysis & Interpretation", id="collapse-corr-button", className="mb-3"),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(dcc.Markdown("""
                #### Clustered Correlation Analysis
                This heatmap shows the correlation between the daily percentage returns of major global stock indices. A value of **1.0** (dark red) means two indices move perfectly together, while **-1.0** (dark blue) means they move in opposite directions.

                **Methodology:**
                To reveal underlying patterns, the indices have been reordered using a hierarchical clustering algorithm ('Ward's method'). This algorithm groups indices that have similar correlation patterns, placing them next to each other in the matrix.
                
                **Key Insights:**
                1.  **Regional Blocs:** Observe the dark red squares that form along the diagonal. These represent strong regional correlations. For example, North American indices (S&P 500, NASDAQ) will cluster together, as will major European indices (DAX, FTSE).
                2.  **Global Integration:** The generally reddish-to-light-tan hue across the matrix indicates that most global equity markets have a positive correlation. A major event in one large market tends to affect others in the same direction.
                3.  **Diversification Opportunities:** Look for blue or dark tan areas. For instance, the correlation between a broad stock index and a specific commodity index (like Gold) might be low or negative, highlighting its potential role as a portfolio diversifier.
            """))),
            id="collapse-corr", is_open=False,
        ),
    ]))),

    dbc.Row(dbc.Col(dbc.Spinner(dcc.Graph(id='correlation-graph'))))
], fluid=True)


# --- Callbacks ---
# This callback populates the graph and handles the case where data loading fails
@callback(
    Output('correlation-graph', 'figure'),
    Input('correlation-graph', 'id') # Dummy input to trigger on page load
)
def update_correlation_graph(_):
    if correlation_matrix.empty:
        return go.Figure().update_layout(
            template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            annotations=[dict(text="Error: Could not generate heatmap.", showarrow=False, font_size=20)]
        )
    
    fig = go.Figure(go.Heatmap(
        z=correlation_matrix.values, x=correlation_matrix.columns, y=correlation_matrix.index,
        colorscale='RdBu_r', zmin=-1, zmax=1, hoverongaps=False
    ))
    fig.update_layout(
        title={'text': 'Clustered Correlation Matrix of Global Index Daily Returns', 'font': {'color': 'white', 'size': 20}, 'x': 0.5},
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        height=900, xaxis_showgrid=False, yaxis_showgrid=False,
        yaxis_autorange='reversed'
    )
    fig.update_xaxes(tickangle=45)
    return fig

# Callback for the collapse button
@callback(
    Output("collapse-corr", "is_open"),
    Input("collapse-corr-button", "n_clicks"),
    State("collapse-corr", "is_open"),
)
def toggle_collapse(n, is_open):
    if n: return not is_open
    return is_open