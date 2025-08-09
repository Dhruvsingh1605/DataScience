import pandas as pd
import sqlite3

df = pd.read_csv('vgsales_cleaned.csv')

conn = sqlite3.connect('games.db')

df.to_sql('game_sales', conn, if_exists='replace', index=False)

print("✅ CSV loaded into SQL table 'game_sales'")
