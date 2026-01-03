import pandas as pd
import dash
from dash import dcc, html, ctx, callback, Input, Output, State, clientside_callback
import os
import dash_bootstrap_components as dbc
import sys

# --- Register Page ---
dash.register_page(__name__, name="Animated Growth", title="Animated Growth Chart")

# --- Data Loading and Preparation ---
df_final, unique_dates, unique_continents, date_marks, data_for_store = pd.DataFrame(), [], [], {}, []

try:
    # Import shared data loader
    try:
        from data_loader import global_data_loader
    except ImportError:
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from data_loader import global_data_loader

    # 1. Load index metadata
    df_indices = global_data_loader.load_indices()

    # 2. Load price data from shared loader
    df_prices = global_data_loader.load_prices()

    # 3. Process data
    if not df_prices.empty:
        df_indices = df_indices[df_indices['Ticker'] != '^MERV'].copy()
        index_tickers = df_indices['Ticker'].tolist()
        df_prices = df_prices[df_prices.columns.intersection(index_tickers)]

        # Use 'ME' for month-end frequency
        df_monthly = df_prices.resample('ME').last()
        df_growth = (df_monthly / df_monthly.iloc[0] - 1) * 100
        df_melted = df_growth.reset_index().melt(id_vars='Date', var_name='Ticker', value_name='Cumulative Growth (%)')
        
        df_final = pd.merge(df_melted, df_indices, on='Ticker')
        df_final['Date'] = df_final['Date'].dt.strftime('%Y-%m-%d')
        df_final.dropna(subset=['Cumulative Growth (%)'], inplace=True)
        
        unique_dates = sorted(df_final['Date'].unique())
        unique_continents = sorted(df_final['Continent'].unique())
        date_marks = {i: date[:4] for i, date in enumerate(unique_dates) if i % 12 == 0 or i == len(unique_dates) - 1}
        data_for_store = df_final.to_dict('records')

except Exception as e:
    print(f"Error loading data in Animated Growth page: {e}")


# --- Page Layout ---
layout = dbc.Container([
    html.H1("Stock Index Cumulative Growth Over Time", className="text-center my-4"),
    dbc.Card(dbc.CardBody(
        dcc.Dropdown(
            id='continent-filter-animation',
            options=[{'label': c, 'value': c} for c in unique_continents] + [{'label': 'All Continents', 'value': 'All'}],
            value=['All'], multi=True, placeholder="Select continents"
        )
    ), className="p-3 mb-4"),

    dbc.Row(dbc.Col(html.Div([
        dbc.Button("Show Analysis & Interpretation", id="collapse-animation-button", className="mb-3"),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(id="animation-interpretation-text")),
            id="collapse-animation", is_open=False,
        ),
    ]))),

    dbc.Row(dbc.Col(dbc.Spinner(dcc.Graph(id='growth-chart-animation', style={'height': '70vh'})))),
    
    html.Div([
        dbc.Button('▶ Play', id='play-button-animation', n_clicks=0, className="me-2"),
        dbc.Button('❚❚ Pause', id='pause-button-animation', n_clicks=0)
    ], className="text-center mt-3 mb-2"),
    
    dcc.Slider(
        id='date-slider-animation', min=0, max=len(unique_dates) - 1, value=len(unique_dates) - 1,
        marks=date_marks, step=1, updatemode='mouseup'
    ),
    
    # Hidden components for data and interactivity
    dcc.Store(id='data-store-animation', data=data_for_store),
    dcc.Store(id='unique-dates-store-animation', data=unique_dates),
    dcc.Interval(id='interval-component-animation', interval=200, n_intervals=0, disabled=True)
], fluid=True)


# --- Callbacks for Interactivity and Interpretation ---

# Callback to control the play/pause state
@callback(
    Output('interval-component-animation', 'disabled'), 
    [Input('play-button-animation', 'n_clicks'), Input('pause-button-animation', 'n_clicks')]
)
def toggle_animation(play_clicks, pause_clicks):
    if ctx.triggered_id == 'play-button-animation': return False
    return True

# Callback to advance the slider during play
@callback(
    Output('date-slider-animation', 'value'), 
    Input('interval-component-animation', 'n_intervals'), 
    [State('date-slider-animation', 'value'), State('date-slider-animation', 'max')]
)
def advance_slider(n, current_val, max_val):
    if current_val < max_val: return current_val + 1
    return max_val

# Callback for the interpretation collapse button
@callback(
    Output("collapse-animation", "is_open"),
    Input("collapse-animation-button", "n_clicks"),
    State("collapse-animation", "is_open"),
)
def toggle_collapse(n, is_open):
    if n: return not is_open
    return is_open

# Callback for the dynamic interpretation text
@callback(
    Output("animation-interpretation-text", "children"),
    Input('continent-filter-animation', 'value')
)
def update_interpretation(selected_continents):
    if not selected_continents or df_final.empty:
        return "Select continents to view analysis."

    continents_text = "all continents"
    if 'All' not in selected_continents:
        continents_text = f"the selected continents: {', '.join(selected_continents)}"
        filtered_df = df_final[df_final['Continent'].isin(selected_continents)]
    else:
        filtered_df = df_final
    
    if filtered_df.empty:
        return "No data for the selected continents."
        
    final_date_data = filtered_df[filtered_df['Date'] == unique_dates[-1]]
    final_performance = final_date_data.set_index('Index Name')['Cumulative Growth (%)']
    best_performer = final_performance.idxmax()
    worst_performer = final_performance.idxmin()

    return dcc.Markdown(f"""
    #### Long-Term Growth Animation Analysis
    This animation visualizes the cumulative, long-term performance of stock indices, rebased to a common starting point. It's a powerful tool for comparing the growth trajectories of different markets over a multi-year horizon.

    **Key Insights for {continents_text}:**
    *   **Best Overall Performer:** At the end of the period, the **{best_performer}** index showed the highest cumulative growth.
    *   **Worst Overall Performer:** Conversely, the **{worst_performer}** index had the lowest cumulative growth.
    
    By observing the paths, one can identify periods of significant divergence, such as the outperformance of technology-heavy indices in certain years, or periods of global downturn where most indices fall in unison.
    """)


# High-performance clientside callback for the chart itself
clientside_callback(
    """
    function(slider_value, selected_continents, all_data, unique_dates) {
        const current_date = unique_dates[slider_value];
        let filtered_data = all_data.filter(d => d.Date <= current_date);
        if (selected_continents && selected_continents.length > 0 && !selected_continents.includes('All')) {
            filtered_data = filtered_data.filter(d => selected_continents.includes(d.Continent));
        }
        const lines_data = {};
        filtered_data.forEach(d => {
            const name = d['Index Name'];
            const growth = parseFloat(d['Cumulative Growth (%)']);
            if (!isNaN(growth) && isFinite(growth)) {
                 if (!lines_data[name]) {
                    lines_data[name] = {x: [], y: [], name: `${name} (${d.Continent})`, type: 'scatter', mode: 'lines', line: {width: 2}};
                }
                lines_data[name].x.push(d.Date);
                lines_data[name].y.push(growth);
            }
        });
        const line_traces = Object.values(lines_data);
        const marker_traces = line_traces.map(trace => ({
            x: [trace.x[trace.x.length - 1]], y: [trace.y[trace.y.length - 1]], name: trace.name, 
            type: 'scatter', mode: 'markers', marker: { size: 8 }, showlegend: false
        }));
        const layout = {
            title: `Cumulative Growth up to ${current_date}`,
            xaxis: { title: 'Date', range: [unique_dates[0], unique_dates[unique_dates.length - 1]], autorange: false },
            yaxis: { title: 'Cumulative Growth (%)', autorange: true, zeroline: true, zerolinewidth: 2, zerolinecolor: 'grey' },
            hovermode: 'closest',
            legend: { orientation: 'v', x: 1.02, xanchor: 'left', y: 1 },
            template: 'plotly_dark',
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)'
        };
        return {data: line_traces.concat(marker_traces), layout: layout};
    }
    """,
    Output('growth-chart-animation', 'figure'),
    [Input('date-slider-animation', 'value'), Input('continent-filter-animation', 'value')],
    [State('data-store-animation', 'data'), State('unique-dates-store-animation', 'data')]
)