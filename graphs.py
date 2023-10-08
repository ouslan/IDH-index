from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd

class Graphs:

    def __init__(self, path, title):
        self.path = pd.read_csv(path)
        self.title = title

    def plot_graph(self):
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=self.path['Year'], y=self.path['index'], name=f'{self.title} Index', mode='lines'))
        # make an anotation for Huricane Maria
        fig.add_annotation(x=2017, y=self.path.loc[self.path['Year'] == 2017].iat[0, 1], text=f'Huracan Maria', showarrow=True,
                           font=dict(family="Courier New, monospace",
                                     size=16,
                                     color="#ffffff"),
                           align="center", arrowhead=2, arrowsize=1, arrowwidth=2, arrowcolor="#636363", ax=20, ay=-30, 
                           bordercolor="#c7c7c7", borderwidth=2, borderpad=4, bgcolor="#ff7f0e", opacity=0.8)
        # make an anotation for Pandemic
        fig.add_annotation(x=2020, y=self.path.loc[self.path['Year'] == 2020].iat[0, 1], text=f'La Pandemia', showarrow=True,
                           font=dict(family="Courier New, monospace",
                                     size=16,
                                     color="#ffffff"),
                           align="center", arrowhead=2, arrowsize=1, arrowwidth=2, arrowcolor="#636363", ax=-20, ay=30, 
                           bordercolor="#c7c7c7", borderwidth=2, borderpad=4, bgcolor="#ff7f0e", opacity=0.8)
        fig.update_layout(title=self.title)
        fig.update_xaxes(title_text='Year')
        fig.update_yaxes(title_text=f'{self.title} Index')
        fig.update_layout(height=800, width=1200)
        fig.show()

if __name__ == '__main__':
    path = 'Data/edu_index.csv'
    title = 'Health'
    g = Graphs(path, title)
    g.plot_graph()