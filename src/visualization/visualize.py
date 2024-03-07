import pandas as pd
import plotext as plt

class Graphs:

    def __init__(self, path):
        self.data = pd.read_csv(path)

    def plot_graph(self, y, x1, x2, x3, x4, title):
        plt.plot(self.data[y], self.data[x1], color='red', label=x1)
        plt.plot(self.data[y], self.data[x2], color='blue', label=x2)
        plt.plot(self.data[y], self.data[x3], color='green', label=x3)
        plt.plot(self.data[y], self.data[x4], color='black', label=x4)
        plt.title(title)
        plt.show()


        

if __name__ == '__main__':
    g = Graphs('/home/coder/Documents/Github/IDH-index/data/processed/idh_index.csv')
    g.plot_graph('Year', 'index', 'health_index', 'income_index', 'edu_index', 'IDH Index')