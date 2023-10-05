from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd

def bbands(data, window=36, no_of_std=1):
    rolling_mean = data.rolling(window).mean()
    rolling_std  = data.rolling(window).std()
    upper_band = rolling_mean + (rolling_std * no_of_std)
    lower_band = rolling_mean - (rolling_std * no_of_std)
    return rolling_mean, upper_band, lower_band


class Graphs:

    def __init__(self, index):
        self.data = index
        data = pd.read_csv(self.data)

    def gen_graph(self):
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=data['Year'], y=data['index'], name='Value Growth'))
        fig.add_trace(go.Scatter(x=data['Year'], y=bbands(data['index'])[0], name='Mean'))
        fig.add_trace(go.Scatter(x=data['Year'], y=bbands(data['index'])[1], name='Upper'))
        fig.add_trace(go.Scatter(x=data['Year'], y=bbands(data['index'])[2], name='Lower'))
        
        fig.add_trace(go.Scatter(x=data[data['index'] > bbands(data['index'])[1]]['Year'], 
                                 y=data[data['index'] > bbands(data['index'])[1]]['index'], 
                                 mode='markers', 
                                 marker=dict(color='Yellow',size=8,line=dict(color='DarkSlateGrey',width=2)),# marker
                                 name='Above 1 std'))
        
        fig.add_trace(go.Scatter(x=data[data['index'] > bbands(data['index'],no_of_std=2)[1]]['Year'],
                                 y=data[data['index'] > bbands(data['index'],no_of_std=2)[1]]['index'],
                                 mode='markers',
                                 marker=dict(color='Orange',size=8, line=dict(color='DarkSlateGrey', width=2)),# marker
                                 name='Above 2 std'))
        
        fig.add_trace(go.Scatter(x=data[data['index'] > bbands(data['index'],no_of_std=3)[1]]['Year'],
                                 y=data[data['index'] > bbands(data['index'],no_of_std=3)[1]]['index'],
                                 mode='markers',
                                 marker=dict(color='Red',size=8,line=dict(color='DarkSlateGrey',width=2)), # marker
                                 name='Above 3 std'))
        
        fig.update_layout(title=f'Bollinger Bands for HTS {self.HTS_code}', xaxis_title='Year', yaxis_title='Value')
        return fig.show()
