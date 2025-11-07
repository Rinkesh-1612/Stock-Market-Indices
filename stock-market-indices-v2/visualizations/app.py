# app.py
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc

# --- Initialize the App with Pages, Bootstrap Theme, and Suppress Exceptions ---
app = dash.Dash(
    __name__, 
    use_pages=True, 
    external_stylesheets=[dbc.themes.CYBORG], # Dark theme
    suppress_callback_exceptions=True
)
server = app.server

# --- Define the Main Layout (The "Shell" of the App) ---
app.layout = html.Div([
    
    # --- Navigation Bar ---
    dbc.Navbar(
        dbc.Container(
            [
                html.A(
                    dbc.Row(
                        [
                            # Optional: Add a logo image in your assets folder
                            # dbc.Col(html.Img(src=app.get_asset_url('logo.png'), height="30px")),
                            dbc.Col(dbc.NavbarBrand("Global Stock Market Analysis", className="ms-2")),
                        ],
                        align="center",
                        className="g-0",
                    ),
                    href="/",
                    style={"textDecoration": "none"},
                ),
                dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                dbc.Collapse(
                    dbc.Nav(
                        # Automatically create a NavLink for each page
                        [
                            dbc.NavLink(page['name'], href=page['relative_path'], active="exact")
                            for page in dash.page_registry.values()
                        ],
                        className="ms-auto",
                        navbar=True
                    ),
                    id="navbar-collapse",
                    navbar=True,
                ),
            ],
            fluid=True
        ),
        color="dark",
        dark=True,
        className="mb-4" # Margin bottom for spacing
    ),

    # --- Shared Memory for Cross-Filtering ---
    # This component allows pages to communicate with each other
    dcc.Store(id='shared-memory-store', storage_type='session'),

    # --- Page Content Container ---
    # The content of each page will be rendered inside this container
    dbc.Container(
        dash.page_container,
        fluid=True
    )
])

# --- Run the App ---
if __name__ == '__main__':
    app.run(debug=True)