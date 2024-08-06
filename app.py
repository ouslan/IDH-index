from dash import Dash, html, dcc, dash_table, Input, Output, State
from src.data.data_process import DataProcess
import plotly.express as px
import pandas as pd

DataProcess(end_year=2023, debug=True)

# Data initialization
df = pd.read_csv('data/processed/idh_index.csv')

# Initialize the Dash app
app = Dash(__name__, meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1.0"}])
app.title = "Human Development Index"
server = app.server

# Define the layout of the app
app.layout = html.Div([
    html.Div(id="header", children=[
        html.H1("Human Development Index Analysis"),
        dcc.Markdown('''
            #### Overview

            - This is a simple application that visualizes the Human Development Index (HDI) over time for Puerto Rico.
            - The source code for the project as well as the replication package for the dataset and sources can be found
            [here](https://github.com/ouslan/IDH-index).
            - The data and full specification of the HDI can be found [here](https://hdr.undp.org/data-center/human-development-index#/indicies/HDI)
        '''),
        html.Button("Download Data", id="btn-download-txt"),
        dcc.Tabs(id='tabs-nav', value='data-tab', children=[
            dcc.Tab(label='Data', value='data-tab'),
            dcc.Tab(label='IDH Graph', value='graph-tab'),
            dcc.Tab(label='IDH Adjusted Graph', value='adj-tab'),
        ]),
    ]),

    dcc.Download(id="download-text"),

    html.Div(id='tabs-content')
    
])

# Define callbacks for each tab
@app.callback(
    Output('tabs-content', 'children'),
    [Input('tabs-nav', 'value')]
)
def render_content(tab):
    if tab == 'data-tab':
        return html.Div([
            dash_table.DataTable(
                data=df.to_dict('records'),
                page_size=6
            )
        ], style={'padding': '10px'})
    elif tab == 'graph-tab':
        return html.Div([
            dcc.Graph(
                id='idh-graph',
                figure=px.line(df, x='year', y=["index", "income_index", "edu_index", "health_index"], title='Human Development Index', range_y=[0, 1]),
                style={'width': '100%', 'height': '80vh'}
            )
        ], style={'padding': '10px'})
    elif tab == 'adj-tab':
        return html.Div([
            dcc.Graph(
                id='idh-adj-graph',
                figure=px.line(df, x='year', y=["index_adjusted", "income_index_adjusted", "edu_index_adjusted", "health_index_adjusted"], title='Human Development Index', range_y=[0, 1]),
                style={'width': '100%', 'height': '80vh'}
            )
        ], style={'padding': '10px'})

@app.callback(
    Output("download-text", "data"),
    [Input("btn-download-txt", "n_clicks")],
    prevent_initial_call=True
)
def download_data(n_clicks):
    if n_clicks:
        # Convert DataFrame to CSV
        csv_string = df.to_csv(index=False)  # Use tab separator for text file
        return dict(content=csv_string, filename="idh_index_data.csv")

# Run the app
if __name__ == '__main__':
    app.run_server(debug=False, host="0.0.0.0", port=7050)
